using System;
using System.Threading;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.Server;

namespace Hangfire.Redis;

/// <summary>
/// 拉取作业观察者
/// </summary>
internal class FetchedJobsWatcher : IServerComponent
{
    /// <summary>
    /// 隐形超时时间
    /// </summary>
    private readonly TimeSpan _invisibilityTimeout;

    /// <summary>
    /// 日志
    /// </summary>
    private static readonly ILog Logger = LogProvider.GetLogger(typeof(FetchedJobsWatcher));

    /// <summary>
    /// Redis存储
    /// </summary>
    private readonly RedisStorage _storage;

    /// <summary>
    /// 拉取作业观察者选项配置
    /// </summary>
    private readonly FetchedJobsWatcherOptions _options;

    /// <summary>
    /// 初始化一个<see cref="FetchedJobsWatcher"/>类型的实例
    /// </summary>
    /// <param name="storage">Redis存储</param>
    /// <param name="invisibilityTimeout">隐形超时时间</param>
    public FetchedJobsWatcher(RedisStorage storage, TimeSpan invisibilityTimeout) : this(storage, invisibilityTimeout, new FetchedJobsWatcherOptions()) { }

    /// <summary>
    /// 初始化一个<see cref="FetchedJobsWatcher"/>类型的实例
    /// </summary>
    /// <param name="storage">Redis存储</param>
    /// <param name="invisibilityTimeout">隐形超时时间</param>
    /// <param name="options">拉取作业观察者选项配置</param>
    public FetchedJobsWatcher(RedisStorage storage, TimeSpan invisibilityTimeout, FetchedJobsWatcherOptions options)
    {
        if (invisibilityTimeout.Ticks <= 0)
            throw new ArgumentOutOfRangeException(nameof(invisibilityTimeout), "Invisibility timeout duration should be positive.");
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _invisibilityTimeout = invisibilityTimeout;
    }

    /// <summary>
    /// 执行
    /// </summary>
    /// <param name="cancellationToken">取消令牌</param>
    public void Execute(CancellationToken cancellationToken)
    {
        using (var connection = (RedisConnection)_storage.GetConnection())
        {
            var queues = connection.RedisClient.SMembers(_storage.GetRedisKey("queues"));
            foreach (var queue in queues)
                ProcessQueue(queue, connection);
        }
        cancellationToken.WaitHandle.WaitOne(_options.SleepTimeout);
    }

    /// <summary>
    /// 处理队列
    /// </summary>
    /// <param name="queue">队列</param>
    /// <param name="connection">Redis连接</param>
    private void ProcessQueue(string queue, RedisConnection connection)
    {
        // 一次仅允许一台服务器处理指定队列中的超时作业。
        Logger.DebugFormat("Acquiring the lock for the fetched list of the '{0}' queue...", queue);
        using (connection.RedisClient.Lock(_storage.GetRedisKey($"queue:{queue}:dequeued:lock"), (int)_options.FetchedLockTimeout.TotalSeconds))
        {
            Logger.DebugFormat("Looking for timed out jobs in the '{0}' queue...", queue);
            var jobIds = connection.RedisClient.LRange(_storage.GetRedisKey($"queue:{queue}:dequeued"), 0, -1);
            var requeued = 0;
            foreach (var jobId in jobIds)
            {
                if (RequeueJobIfTimedOut(connection, jobId, queue))
                    requeued++;
            }
            if (requeued == 0)
                Logger.DebugFormat("No timed out jobs were found in the '{0}' queue", queue);
            else
                Logger.InfoFormat(
                    "{0} timed out jobs were found in the '{1}' queue and re-queued.",
                    requeued,
                    queue);
        }
    }

    /// <summary>
    /// 如果已经超时，则重新将作业入队
    /// </summary>
    /// <param name="connection">Redis连接</param>
    /// <param name="jobId">作业标识</param>
    /// <param name="queue">队列</param>
    private bool RequeueJobIfTimedOut(RedisConnection connection, string jobId, string queue)
    {
        var flags = connection.RedisClient.HMGet(_storage.GetRedisKey($"job:{jobId}"),
            new string[] { "Fetched", "Checked" });
        var fetched = flags[0];
        var @checked = flags[1];
        if (string.IsNullOrEmpty(fetched) && string.IsNullOrEmpty(@checked))
        {
            connection.RedisClient.HSet(_storage.GetRedisKey($"job:{jobId}"), "Checked",
                JobHelper.SerializeDateTime(DateTime.UtcNow));
        }
        else
        {
            if (TimedOutByFetchedTime(fetched) || TimedOutByCheckedTime(fetched, @checked))
            {
                var fetchedJob = new RedisFetchedJob(_storage, connection.RedisClient, jobId, queue);
                fetchedJob.Dispose();
                return true;
            }
        }
        return false;
    }

    /// <summary>
    /// 是否拉取时间超时
    /// </summary>
    /// <param name="fetchedTimestamp">拉取时间戳</param>
    private bool TimedOutByFetchedTime(string fetchedTimestamp) =>
        !string.IsNullOrEmpty(fetchedTimestamp) &&
        DateTime.UtcNow - JobHelper.DeserializeDateTime(fetchedTimestamp) > _invisibilityTimeout;

    /// <summary>
    /// 是否检查时间超时
    /// </summary>
    /// <param name="fetchedTimestamp">拉取时间戳</param>
    /// <param name="checkedTimestamp">检查时间戳</param>
    private bool TimedOutByCheckedTime(string fetchedTimestamp, string checkedTimestamp)
    {
        if (!string.IsNullOrEmpty(fetchedTimestamp))
            return false;
        return !string.IsNullOrEmpty(checkedTimestamp) &&
               DateTime.UtcNow - JobHelper.DeserializeDateTime(checkedTimestamp) > _options.CheckedTimeout;
    }
}