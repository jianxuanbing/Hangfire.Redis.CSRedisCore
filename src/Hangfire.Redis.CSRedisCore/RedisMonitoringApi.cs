using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CSRedis;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

namespace Hangfire.Redis
{
    /// <summary>
    /// Redis监控API
    /// </summary>
    public class RedisMonitoringApi : IMonitoringApi
    {
        /// <summary>
        /// Redis存储
        /// </summary>
        private readonly RedisStorage _storage;

        /// <summary>
        /// Redis客户端
        /// </summary>
        private readonly CSRedisClient _redisClient;

        /// <summary>
        /// 初始化一个<see cref="RedisMonitoringApi"/>类型的实例
        /// </summary>
        /// <param name="storage">Redis存储</param>
        /// <param name="redisClient">Redis客户端</param>
        public RedisMonitoringApi([NotNull] RedisStorage storage, [NotNull] CSRedisClient redisClient)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _redisClient = redisClient ?? throw new ArgumentNullException(nameof(redisClient));
        }

        /// <summary>
        /// 获取队列列表
        /// </summary>
        public IList<QueueWithTopEnqueuedJobsDto> Queues() =>
            UseConnection(redis =>
            {
                var queues = redis.SMembers(_storage.GetRedisKey("queues"));

                var result = new List<QueueWithTopEnqueuedJobsDto>(queues.Length);

                foreach (var queue in queues)
                {
                    string[] firstJobIds = null;
                    long length = 0;
                    long fetched = 0;


                    Task[] tasks = new Task[3];
                    tasks[0] = _redisClient.LRangeAsync(
                            _storage.GetRedisKey($"queue:{queue}"), -5, -1)
                        .ContinueWith(x => firstJobIds = x.Result);

                    tasks[1] = _redisClient.LLenAsync(_storage.GetRedisKey($"queue:{queue}"))
                        .ContinueWith(x => length = x.Result);

                    tasks[2] = _redisClient.LLenAsync(_storage.GetRedisKey($"queue:{queue}:dequeued"))
                        .ContinueWith(x => fetched = x.Result);
                    Task.WaitAll(tasks);

                    var jobs = GetJobsWithProperties(
                        redis,
                        firstJobIds,
                        new[] { "State" },
                        new[] { "EnqueuedAt", "State" },
                        (job, jobData, state) => new EnqueuedJobDto
                        {
                            Job = job,
                            State = jobData[0],
                            EnqueuedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                            InEnqueuedState = jobData[0].Equals(state[1], StringComparison.OrdinalIgnoreCase)
                        });

                    result.Add(new QueueWithTopEnqueuedJobsDto
                    {
                        Name = queue,
                        FirstJobs = jobs,
                        Length = length,
                        Fetched = fetched
                    });
                }

                return result;
            });

        /// <summary>
        /// 获取服务器列表
        /// </summary>
        public IList<ServerDto> Servers() =>
            UseConnection(redis =>
            {
                var serverNames = redis.SMembers(_storage.GetRedisKey("servers"));


                if (serverNames.Length == 0)
                    return new List<ServerDto>();

                var servers = new List<ServerDto>();

                foreach (var serverName in serverNames)
                {
                    var queue = redis.LRange(_storage.GetRedisKey($"server:{serverName}:queues"), 0, -1);

                    var server = redis.HMGet(_storage.GetRedisKey($"server:{serverName}"), new string[] { "WorkerCount", "StartedAt", "Heartbeat" });
                    if (server[0] == null)
                        continue;   // skip removed server

                    servers.Add(new ServerDto
                    {
                        Name = serverName,
                        WorkersCount = int.Parse(server[0]),
                        Queues = queue,
                        StartedAt = JobHelper.DeserializeDateTime(server[1]),
                        Heartbeat = JobHelper.DeserializeNullableDateTime(server[2])
                    });
                }

                return servers;
            });

        /// <summary>
        /// 获取作业详情
        /// </summary>
        /// <param name="jobId">作业标识</param>
        public JobDetailsDto JobDetails([NotNull] string jobId)
        {
            if (jobId == null) 
                throw new ArgumentNullException(nameof(jobId));

            return UseConnection(redis =>
            {
                var job = redis.HGetAll(_storage.GetRedisKey($"job:{jobId}"));

                if (job.Count == 0) return null;

                var hiddenProperties = new[] { "Type", "Method", "ParameterTypes", "Arguments", "State", "CreatedAt", "Fetched" };

                var history = redis
                    .LRange(_storage.GetRedisKey($"job:{jobId}:history"), 0, -1)
                    .Select(SerializationHelper.Deserialize<Dictionary<string, string>>)
                    .ToList();

                history.Reverse();

                var stateHistory = new List<StateHistoryDto>(history.Count);
                foreach (var entry in history)
                {
                    var stateData = new Dictionary<string, string>(entry, StringComparer.OrdinalIgnoreCase);
                    var dto = new StateHistoryDto
                    {
                        StateName = stateData["State"],
                        Reason = stateData.ContainsKey("Reason") ? stateData["Reason"] : null,
                        CreatedAt = JobHelper.DeserializeDateTime(stateData["CreatedAt"]),
                    };

                    stateData.Remove("State");
                    stateData.Remove("Reason");
                    stateData.Remove("CreatedAt");

                    dto.Data = stateData;
                    stateHistory.Add(dto);
                }

                // For compatibility
                if (!job.ContainsKey("Method")) job.Add("Method", null);
                if (!job.ContainsKey("ParameterTypes")) job.Add("ParameterTypes", null);

                return new JobDetailsDto
                {
                    Job = TryToGetJob(job["Type"], job["Method"], job["ParameterTypes"], job["Arguments"]),
                    CreatedAt =
                        job.ContainsKey("CreatedAt")
                            ? JobHelper.DeserializeDateTime(job["CreatedAt"])
                            : (DateTime?)null,
                    Properties =
                        job.Where(x => !hiddenProperties.Contains(x.Key)).ToDictionary(x => x.Key, x => x.Value),
                    History = stateHistory
                };
            });
        }

        /// <summary>
        /// 获取统计信息
        /// </summary>
        public StatisticsDto GetStatistics() =>
            UseConnection(redis =>
            {
                var stats = new StatisticsDto();
                
                var queues = redis.SMembers(_storage.GetRedisKey("queues"));
                using (var pipe = redis.StartPipe())
                {
                    pipe.SCard(_storage.GetRedisKey("servers"))
                        .SCard(_storage.GetRedisKey("queues"))
                        .ZCard(_storage.GetRedisKey("schedule"))
                        .ZCard(_storage.GetRedisKey("processing"))
                        .Get(_storage.GetRedisKey("stats:succeeded"))
                        .ZCard(_storage.GetRedisKey("failed"))
                        .Get(_storage.GetRedisKey("stats:deleted"))
                        .ZCard(_storage.GetRedisKey("recurring-jobs"));
                    foreach (var queue in queues)
                    {
                        pipe.LLen(_storage.GetRedisKey($"queue:{queue}"));
                    }

                    var result = pipe.EndPipe();
                    if (result.Length >= 8)
                    {
                        stats.Servers = result[0] == null ? 0 : Convert.ToInt64(result[0]);
                        stats.Queues = result[1] == null ? 0 : Convert.ToInt64(result[1]);
                        stats.Scheduled = result[2] == null ? 0 : Convert.ToInt64(result[2]);
                        stats.Processing = result[3] == null ? 0 : Convert.ToInt64(result[3]);
                        stats.Succeeded = result[4] == null ? 0 : Convert.ToInt64(result[4]);
                        stats.Failed = result[5] == null ? 0 : Convert.ToInt64(result[5]);
                        stats.Deleted = result[6] == null ? 0 : Convert.ToInt64(result[6]);
                        stats.Recurring = result[7] == null ? 0 : Convert.ToInt64(result[7]);
                        for (var i = 8; i < result.Length; i++)
                        {
                            stats.Enqueued += Convert.ToInt64(result[i]);
                        }
                    }
                }

                return stats;
            });

        /// <summary>
        /// 已入队的作业
        /// </summary>
        /// <param name="queue">队列名</param>
        /// <param name="from">开始范围</param>
        /// <param name="count">数量</param>
        public JobList<EnqueuedJobDto> EnqueuedJobs([NotNull] string queue, int @from, int count)
        {
            if (queue == null) 
                throw new ArgumentNullException(nameof(queue));
            return UseConnection(redis =>
            {
                var jobIds = redis.LRange(_storage.GetRedisKey($"queue:{queue}"), from, from + count - 1);

                return GetJobsWithProperties(
                    redis,
                    jobIds,
                    new[] { "State" },
                    new[] { "EnqueuedAt", "State" },
                    (job, jobData, state) => new EnqueuedJobDto
                    {
                        Job = job,
                        State = jobData[0],
                        EnqueuedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        InEnqueuedState = jobData[0].Equals(state[1], StringComparison.OrdinalIgnoreCase)
                    });
            });
        }

        /// <summary>
        /// 已拉取的作业
        /// </summary>
        /// <param name="queue">队列名</param>
        /// <param name="from">开始范围</param>
        /// <param name="count">数量</param>
        public JobList<FetchedJobDto> FetchedJobs(string queue, int @from, int count)
        {
            if (queue == null) 
                throw new ArgumentNullException(nameof(queue));
            return UseConnection(redis =>
            {
                var jobIds = redis.LRange(_storage.GetRedisKey($"queue:{queue}:dequeued"), from, from + count - 1);

                return GetJobsWithProperties(
                    redis,
                    jobIds,
                    new[] { "State", "Fetched" },
                    null,
                    (job, jobData, state) => new FetchedJobDto
                    {
                        Job = job,
                        State = jobData[0],
                        FetchedAt = JobHelper.DeserializeNullableDateTime(jobData[1])
                    });
            });
        }

        /// <summary>
        /// 处理中的作业
        /// </summary>
        /// <param name="from">开始范围</param>
        /// <param name="count">数量</param>
        public JobList<ProcessingJobDto> ProcessingJobs(int @from, int count) =>
            UseConnection(redis =>
            {
                var jobIds = redis.ZRange(_storage.GetRedisKey("processing"), @from, @from + count - 1);
                return new JobList<ProcessingJobDto>(GetJobsWithProperties(redis,
                        jobIds,
                        null,
                        new[] {"StartedAt", "ServerName", "ServerId", "State"},
                        (job, jobData, state) =>
                            new ProcessingJobDto
                            {
                                ServerId = state[2] ?? state[1],
                                Job = job,
                                StartedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                                InProcessingState =
                                    ProcessingState.StateName.Equals(state[3], StringComparison.OrdinalIgnoreCase)
                            })
                    .Where(x => x.Value?.ServerId != null)
                    .OrderBy(x => x.Value.StartedAt).ToList());
            });

        /// <summary>
        /// 计划的作业
        /// </summary>
        /// <param name="from">开始范围</param>
        /// <param name="count">数量</param>
        public JobList<ScheduledJobDto> ScheduledJobs(int @from, int count) =>
            UseConnection(redis =>
            {
                var scheduledJobs = redis.ZRangeByScoreWithScores(_storage.GetRedisKey("schedule"), "-inf", "+inf", count, @from);
                if (scheduledJobs.Length == 0)
                {
                    return new JobList<ScheduledJobDto>(new List<KeyValuePair<string, ScheduledJobDto>>());
                }

                var jobs = new ConcurrentDictionary<string, List<string>>();
                var states = new ConcurrentDictionary<string, List<string>>();

                var i = 0;
                foreach (var scheduledJob in scheduledJobs)
                {
                    var jobId = scheduledJob;
                    var v1 = _redisClient.HMGet(
                        _storage.GetRedisKey($"job:{jobId}"),
                        new string[] {"Type", "Method", "ParameterTypes", "Arguments"});

                    jobs.TryAdd(jobId.member, v1.ToList());
                    i++;
                    var v2 = _redisClient.HMGet(
                        _storage.GetRedisKey($"job:{jobId}:state"),
                        new string[] {"State", "ScheduledAt"});
                    states.TryAdd(jobId.member, v2.ToList());
                    i++;
                }

                return new JobList<ScheduledJobDto>(scheduledJobs
                    .Select(job => new KeyValuePair<string, ScheduledJobDto>(
                        job.member,
                        new ScheduledJobDto
                        {
                            EnqueueAt = JobHelper.FromTimestamp((long) job.score),
                            Job = TryToGetJob(jobs[job.member][0], jobs[job.member][1], jobs[job.member][2],
                                jobs[job.member][3]),
                            ScheduledAt =
                                states[job.member].Count > 1
                                    ? JobHelper.DeserializeNullableDateTime(states[job.member][1])
                                    : null,
                            InScheduledState =
                                ScheduledState.StateName.Equals(states[job.member][0],
                                    StringComparison.OrdinalIgnoreCase)
                        }))
                    .ToList());
            });

        /// <summary>
        /// 已成功的作业
        /// </summary>
        /// <param name="from">开始范围</param>
        /// <param name="count">数量</param>
        public JobList<SucceededJobDto> SucceededJobs(int @from, int count) =>
            UseConnection(redis =>
            {
                var succeededJobIds = redis.LRange(_storage.GetRedisKey("succeeded"), @from, @from + count - 1);
                return GetJobsWithProperties(
                    redis,
                    succeededJobIds,
                    null,
                    new[] { "SucceededAt", "PerformanceDuration", "Latency", "State", "Result" },
                    (job, jobData, state) => new SucceededJobDto
                    {
                        Job = job,
                        Result = state[4],
                        SucceededAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        TotalDuration = state[1] != null && state[2] != null
                            ? (long?)long.Parse(state[1]) + (long?)long.Parse(state[2])
                            : null,
                        InSucceededState = SucceededState.StateName.Equals(state[3], StringComparison.OrdinalIgnoreCase)
                    });
            });

        /// <summary>
        /// 已失败的作业
        /// </summary>
        /// <param name="from">开始范围</param>
        /// <param name="count">数量</param>
        public JobList<FailedJobDto> FailedJobs(int @from, int count) =>
            UseConnection(redis =>
            {
                var failedJobIds = redis.ZRange(_storage.GetRedisKey("failed"), @from, @from + count - 1);
                return GetJobsWithProperties(
                    redis,
                    failedJobIds,
                    null,
                    new[] {"FailedAt", "ExceptionType", "ExceptionMessage", "ExceptionDetails", "State", "Reason"},
                    (job, jobData, state) => new FailedJobDto
                    {
                        Job = job,
                        Reason = state[5],
                        FailedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        ExceptionType = state[1],
                        ExceptionMessage = state[2],
                        ExceptionDetails = state[3],
                        InFailedState = FailedState.StateName.Equals(state[4], StringComparison.OrdinalIgnoreCase)
                    });
            });

        /// <summary>
        /// 已删除的作业
        /// </summary>
        /// <param name="from">开始范围</param>
        /// <param name="count">数量</param>
        public JobList<DeletedJobDto> DeletedJobs(int @from, int count) =>
            UseConnection(redis =>
            {
                var deletedJobIds = redis.LRange(_storage.GetRedisKey("deleted"), @from, @from + count - 1);
                return GetJobsWithProperties(
                    redis,
                    deletedJobIds,
                    null,
                    new[] { "DeletedAt", "State" },
                    (job, jobData, state) => new DeletedJobDto
                    {
                        Job = job,
                        DeletedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        InDeletedState = DeletedState.StateName.Equals(state[1], StringComparison.OrdinalIgnoreCase)
                    });
            });

        /// <summary>
        /// 获取已调度计数
        /// </summary>
        public long ScheduledCount() => UseConnection(redis => redis.ZCard(_storage.GetRedisKey("schedule")));

        /// <summary>
        /// 获取已入队计数
        /// </summary>
        /// <param name="queue">队列名</param>
        public long EnqueuedCount([NotNull] string queue)
        {
            if (queue == null)
                throw new ArgumentNullException(nameof(queue));
            return UseConnection(redis => redis.LLen(_storage.GetRedisKey($"queue:{queue}")));
        }

        /// <summary>
        /// 获取已拉取计数
        /// </summary>
        /// <param name="queue">队列名</param>
        public long FetchedCount([NotNull] string queue)
        {
            if (queue == null)
                throw new ArgumentNullException(nameof(queue));
            return UseConnection(redis => redis.LLen(_storage.GetRedisKey($"queue:{queue}:dequeued")));
        }

        /// <summary>
        /// 获取已失败计数
        /// </summary>
        public long FailedCount() => UseConnection(redis => redis.ZCard(_storage.GetRedisKey("failed")));

        /// <summary>
        /// 获取处理中计数
        /// </summary>
        public long ProcessingCount() => UseConnection(redis => redis.ZCard(_storage.GetRedisKey("processing")));

        /// <summary>
        /// 获取已成功计数
        /// </summary>
        public long SucceededListCount() => UseConnection(redis => redis.LLen(_storage.GetRedisKey("succeeded")));

        /// <summary>
        /// 获取已删除计数
        /// </summary>
        public long DeletedListCount() => UseConnection(redis => redis.LLen(_storage.GetRedisKey("deleted")));

        /// <summary>
        /// 获取每日已成功计数
        /// </summary>
        public IDictionary<DateTime, long> SucceededByDatesCount() => UseConnection(redis => GetTimelineStats(_redisClient, "succeeded"));

        /// <summary>
        /// 获取每日已失败计数
        /// </summary>
        public IDictionary<DateTime, long> FailedByDatesCount() => UseConnection(redis => GetTimelineStats(_redisClient, "failed"));

        /// <summary>
        /// 获取每小时已成功计数
        /// </summary>
        public IDictionary<DateTime, long> HourlySucceededJobs() => UseConnection(redis => GetHourlyTimelineStats(_redisClient, "succeeded"));

        /// <summary>
        /// 获取每小时已失败计数
        /// </summary>
        public IDictionary<DateTime, long> HourlyFailedJobs() => UseConnection(redis => GetHourlyTimelineStats(_redisClient, "failed"));

        /// <summary>
        /// 获取每小时统计信息
        /// </summary>
        /// <param name="redis">Redis</param>
        /// <param name="type">类型</param>
        private Dictionary<DateTime, long> GetHourlyTimelineStats([NotNull] CSRedisClient redis, [NotNull] string type)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));
            var endDate = DateTime.UtcNow;
            var dates = new List<DateTime>();
            for (var i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }
            var keys = dates.Select(x => _storage.GetRedisKey($"stats:{type}:{x:yyyy-MM-dd-HH}")).ToArray();
            var valuesMap = redis.GetValuesMap(keys);
            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < dates.Count; i++)
            {
                if (!long.TryParse(valuesMap[valuesMap.Keys.ElementAt(i)], out var value))
                    value = 0;
                result.Add(dates[i], value);
            }
            return result;
        }

        /// <summary>
        /// 获取每天统计信息
        /// </summary>
        /// <param name="redis">Redis</param>
        /// <param name="type">类型</param>
        private Dictionary<DateTime, long> GetTimelineStats([NotNull] CSRedisClient redis, [NotNull] string type)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));
            var endDate = DateTime.UtcNow.Date;
            var startDate = endDate.AddDays(-7);
            var dates = new List<DateTime>();
            while (startDate <= endDate)
            {
                dates.Add(endDate);
                endDate = endDate.AddDays(-1);
            }

            var keys = dates.Select(x => _storage.GetRedisKey($"stats:{type}:{x:yyyy-MM-dd}")).ToArray();
            var valuesMap = redis.GetValuesMap(keys);

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < dates.Count; i++)
            {
                if (!long.TryParse(valuesMap[valuesMap.Keys.ElementAt(i)], out var value))
                    value = 0;
                result.Add(dates[i], value);
            }
            return result;
        }

        /// <summary>
        /// 获取作业列表
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="redis">Redis</param>
        /// <param name="jobIds">作业标识集合</param>
        /// <param name="properties">属性集合</param>
        /// <param name="stateProperties">状态属性集合</param>
        /// <param name="selector">选择器</param>
        private JobList<T> GetJobsWithProperties<T>([NotNull] CSRedisClient redis
            , [NotNull] string[] jobIds
            , string[] properties
            , string[] stateProperties
            , [NotNull] Func<Job, IReadOnlyList<string>, IReadOnlyList<string>, T> selector)
        {
            if (jobIds == null)
                throw new ArgumentNullException(nameof(jobIds));
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));
            if (jobIds.Length == 0)
                return new JobList<T>(new List<KeyValuePair<string, T>>());

            var jobs = new Dictionary<string, Task<string[]>>(jobIds.Length, StringComparer.OrdinalIgnoreCase);
            var states = new Dictionary<string, Task<string[]>>(jobIds.Length, StringComparer.OrdinalIgnoreCase);

            properties = properties ?? new string[0];

            var extendedProperties = properties
                .Concat(new[] { "Type", "Method", "ParameterTypes", "Arguments" })
                .ToArray();

            var tasks = new List<Task>(jobIds.Length * 2);
            foreach (var jobId in jobIds.Distinct())
            {
                var jobTask = redis.HMGetAsync(
                    _storage.GetRedisKey($"job:{jobId}"),
                    extendedProperties);
                tasks.Add(jobTask);
                jobs.Add(jobId, jobTask);

                if (stateProperties != null)
                {
                    var taskStateJob = redis.HMGetAsync(
                        _storage.GetRedisKey($"job:{jobId}:state"),
                        stateProperties);
                    tasks.Add(taskStateJob);
                    states.Add(jobId, taskStateJob);
                }
            }
            Task.WaitAll(tasks.ToArray());

            var jobList = new JobList<T>(jobIds
                .Select(jobId => new
                {
                    JobId = jobId,
                    Job = jobs[jobId].Result,
                    Method = TryToGetJob(
                        jobs[jobId].Result[properties.Length],
                        jobs[jobId].Result[properties.Length + 1],
                        jobs[jobId].Result[properties.Length + 2],
                        jobs[jobId].Result[properties.Length + 3]),
                    State = stateProperties != null ? states[jobId].Result : null
                })
                .Select(x => new KeyValuePair<string, T>(
                    x.JobId,
                    x.Job.Any(y => y != null)
                        ? selector(x.Method, x.Job, x.State)
                        : default(T))));
            return jobList;
        }

        /// <summary>
        /// 使用连接
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="action">操作</param>
        private T UseConnection<T>(Func<CSRedisClient, T> action) => action(_redisClient);

        /// <summary>
        /// 尝试转换获取作业
        /// </summary>
        /// <param name="type">类型</param>
        /// <param name="method">方法</param>
        /// <param name="parameterTypes">参数类型</param>
        /// <param name="arguments">参数</param>
        private static Job TryToGetJob(string type, string method, string parameterTypes, string arguments)
        {
            try
            {
                return new InvocationData(type, method, parameterTypes, arguments).DeserializeJob();
            }
            catch (Exception)
            {
                return null;
            }
        }
    }
}
