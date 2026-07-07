using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using CSRedis;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.Redis;

/// <summary>
/// Redis连接
/// </summary>
internal class RedisConnection : JobStorageConnection
{
    private static readonly HashSet<string> ReservedJobFields = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "Arguments",
        "Checked",
        "CreatedAt",
        "Fetched",
        "Method",
        "ParameterTypes",
        "Queue",
        "State",
        "Type"
    };

    /// <summary>
    /// Redis存储
    /// </summary>
    private readonly RedisStorage _storage;

    /// <summary>
    /// Redis订阅
    /// </summary>
    private readonly RedisSubscription _subscription;

    /// <summary>
    /// 拉取超时时间
    /// </summary>
    private readonly TimeSpan _fetchTimeout;

    /// <summary>
    /// Redis客户端
    /// </summary>
    public CSRedisClient RedisClient { get; }

    /// <summary>
    /// 初始化一个<see cref="RedisConnection"/>类型的实例
    /// </summary>
    /// <param name="storage">Redis存储</param>
    /// <param name="redisClient">Redis客户端</param>
    /// <param name="subscription">Redis订阅</param>
    /// <param name="fetchTimeout">拉取超时时间</param>
    public RedisConnection([NotNull] RedisStorage storage
        , CSRedisClient redisClient
        , [NotNull] RedisSubscription subscription
        , TimeSpan fetchTimeout)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        RedisClient = redisClient ?? throw new ArgumentNullException(nameof(redisClient));
        _subscription = subscription ?? throw new ArgumentNullException(nameof(subscription));
        _fetchTimeout = fetchTimeout;
    }

    /// <summary>
    /// 获取分布式锁
    /// </summary>
    /// <param name="resource">资源</param>
    /// <param name="timeout">超时时间</param>
    public override IDisposable AcquireDistributedLock([NotNull] string resource, TimeSpan timeout) => RedisClient.Lock(GetRequiredRedisKey(resource, nameof(resource)), (int)timeout.TotalSeconds);

    /// <summary>
    /// 公布服务器
    /// </summary>
    /// <param name="serverId">服务器标识</param>
    /// <param name="context">服务器上下文</param>
    public override void AnnounceServer([NotNull] string serverId, [NotNull] ServerContext context)
    {
        if (serverId == null)
            throw new ArgumentNullException(nameof(serverId));
        if (context == null)
            throw new ArgumentNullException(nameof(context));
        var pipe = RedisClient.StartPipe()
            .SAdd(_storage.GetRedisKey("servers"), serverId)
            .HMSet(_storage.GetRedisKey($"server:{serverId}"), new Dictionary<string, string>
            {
                {"WorkerCount", context.WorkerCount.ToString(CultureInfo.InvariantCulture)},
                {"StartedAt", JobHelper.SerializeDateTime(DateTime.UtcNow)},
            }.DicToObjectArray());
        if (context.Queues.Length > 0)
            pipe.RPush(_storage.GetRedisKey($"server:{serverId}:queues"), context.Queues);
        pipe.EndPipe();
    }

    /// <summary>
    /// 创建已过期的作业
    /// </summary>
    /// <param name="job">作业</param>
    /// <param name="parameters">参数字典</param>
    /// <param name="createdAt">创建时间</param>
    /// <param name="expireIn">过期时间</param>
    public override string CreateExpiredJob([NotNull] Job job, [NotNull] IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
    {
        if (job == null)
            throw new ArgumentNullException(nameof(job));
        if (parameters == null)
            throw new ArgumentNullException(nameof(parameters));
        var jobId = Guid.NewGuid().ToString("n");
        var invocationData = InvocationData.SerializeJob(job);

        // 不要修改原始参数
        var storedParameters = new Dictionary<string, string>(parameters)
        {
            {"Type",invocationData.Type},
            {"Method",invocationData.Method },
            { "ParameterTypes", invocationData.ParameterTypes },
            { "Arguments", invocationData.Arguments },
            { "CreatedAt", JobHelper.SerializeDateTime(createdAt) }
        };

        if (!string.IsNullOrWhiteSpace(invocationData.Queue))
            storedParameters["Queue"] = invocationData.Queue;

        RedisClient.StartPipe()
            .HMSet(_storage.GetRedisKey($"job:{jobId}"), storedParameters.DicToObjectArray())
            .Expire(_storage.GetRedisKey($"job:{jobId}"), expireIn)
            .EndPipe();

        return jobId;
    }

    /// <summary>
    /// 创建写入事务
    /// </summary>
    public override IWriteOnlyTransaction CreateWriteTransaction() => new RedisWriteOnlyTransaction(_storage);

    /// <summary>
    /// 释放资源
    /// </summary>
    public override void Dispose()
    {
    }

    /// <summary>
    /// 拉取下一个作业
    /// </summary>
    /// <param name="queues">队列数组</param>
    /// <param name="cancellationToken">取消令牌</param>
    public override IFetchedJob FetchNextJob([NotNull] string[] queues, CancellationToken cancellationToken)
    {
        if (queues == null)
            throw new ArgumentNullException(nameof(queues));

        string jobId = null;
        string queueName = null;
        do
        {
            cancellationToken.ThrowIfCancellationRequested();

            foreach (var queue in queues)
            {
                queueName = queue;
                var queueKey = _storage.GetRedisKey($"queue:{queueName}");
                var fetchedKey = _storage.GetRedisKey($"queue:{queueName}:dequeued");
                jobId = RedisClient.RPopLPush(queueKey, fetchedKey);
                if (jobId != null)
                    break;
            }

            if (jobId == null)
                _subscription.WaitForJob(_fetchTimeout, cancellationToken);
        } while (jobId == null);

        RedisClient.HSet(_storage.GetRedisKey($"job:{jobId}"), "Fetched", JobHelper.SerializeDateTime(DateTime.UtcNow));

        return new RedisFetchedJob(_storage, RedisClient, jobId, queueName);
    }

    /// <summary>
    /// 从 Hash 中获取所有项
    /// </summary>
    /// <param name="key">缓存键</param>
    public override Dictionary<string, string> GetAllEntriesFromHash([NotNull] string key)
    {
        var result = RedisClient.HGetAll(GetRequiredRedisKey(key));
        return result.Count != 0 ? result : null;
    }

    /// <summary>
    /// 从 List 中获取所有项
    /// </summary>
    /// <param name="key">缓存键</param>
    public override List<string> GetAllItemsFromList([NotNull] string key) => RedisClient.LRange(GetRequiredRedisKey(key), 0, -1).ToList();

    /// <summary>
    /// 从 SortedSet 中获取所有项
    /// </summary>
    /// <param name="key">缓存键</param>
    public override HashSet<string> GetAllItemsFromSet([NotNull] string key)
    {
        var result = new HashSet<string>();
        foreach (var item in RedisClient.ZScan(GetRequiredRedisKey(key), 0).Items)
            result.Add(item.member);
        return result;
    }

    /// <summary>
    /// 获取计数器
    /// </summary>
    /// <param name="key">缓存键</param>
    public override long GetCounter([NotNull] string key) => Convert.ToInt64(RedisClient.Get(GetRequiredRedisKey(key)));

    /// <summary>
    /// 获取指定范围内最小值
    /// </summary>
    /// <param name="key">缓存键</param>
    /// <param name="fromScore">开始范围</param>
    /// <param name="toScore">结束范围</param>
    public override string GetFirstByLowestScoreFromSet([NotNull] string key, double fromScore, double toScore) =>
        RedisClient.ZRangeByScore(GetRequiredRedisKey(key), (decimal)fromScore, (decimal)toScore, count: 1, offset: 0)
            .FirstOrDefault();

    /// <summary>
    /// 获取指定范围内最小值的集合
    /// </summary>
    /// <param name="key">缓存键</param>
    /// <param name="fromScore">开始范围</param>
    /// <param name="toScore">结束范围</param>
    /// <param name="count">数量</param>
    public override List<string> GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore, int count) =>
        RedisClient.ZRangeByScore(GetRequiredRedisKey(key), (decimal)fromScore, (decimal)toScore, count: count, offset: 0)
            .ToList();

    /// <summary>
    /// 获取 Hash 计数器
    /// </summary>
    /// <param name="key">缓存键</param>
    public override long GetHashCount([NotNull] string key) => RedisClient.HLen(GetRequiredRedisKey(key));

    /// <summary>
    /// 获取 Hash 剩余时间
    /// </summary>
    /// <param name="key">缓存键</param>
    public override TimeSpan GetHashTtl([NotNull] string key) => TimeSpan.FromSeconds(RedisClient.Ttl(GetRequiredRedisKey(key)));

    /// <summary>
    /// 获取作业数据
    /// </summary>
    /// <param name="jobId">作业标识</param>
    public override JobData GetJobData([NotNull] string jobId)
    {
        if (jobId == null)
            throw new ArgumentNullException(nameof(jobId));
        var storedData = RedisClient.HGetAll(_storage.GetRedisKey($"job:{jobId}"));
        if (storedData.Count == 0)
            return null;

        storedData.TryGetValue("Type", out var type);
        storedData.TryGetValue("Method", out var method);
        storedData.TryGetValue("ParameterTypes", out var parameterTypes);
        storedData.TryGetValue("Arguments", out var arguments);
        storedData.TryGetValue("CreatedAt", out var createdAt);
        storedData.TryGetValue("Queue", out var queue);

        Job job = null;
        JobLoadException loadException = null;
        var invocationData = new InvocationData(type, method, parameterTypes, arguments, queue);

        try
        {
            job = invocationData.DeserializeJob();
        }
        catch (JobLoadException e)
        {
            loadException = e;
        }

        return new JobData
        {
            Job = job,
            InvocationData = invocationData,
            State = storedData.TryGetValue("State", out var state) ? state : null,
            CreatedAt = JobHelper.DeserializeNullableDateTime(createdAt) ?? DateTime.MinValue,
            ParametersSnapshot = storedData
                .Where(x => !ReservedJobFields.Contains(x.Key))
                .ToDictionary(x => x.Key, x => x.Value, StringComparer.OrdinalIgnoreCase),
            LoadException = loadException
        };
    }

    /// <summary>
    /// 获取作业参数
    /// </summary>
    /// <param name="jobId">作业标识</param>
    /// <param name="name">作业名称</param>
    public override string GetJobParameter([NotNull] string jobId, [NotNull] string name)
    {
        if (jobId == null)
            throw new ArgumentNullException(nameof(jobId));
        if (name == null)
            throw new ArgumentNullException(nameof(name));
        return RedisClient.HGet(_storage.GetRedisKey($"job:{jobId}"), name);
    }

    /// <summary>
    /// 获取 List 计数器
    /// </summary>
    /// <param name="key">缓存键</param>
    public override long GetListCount([NotNull] string key) => RedisClient.LLen(GetRequiredRedisKey(key));

    /// <summary>
    /// 获取 List 剩余时间
    /// </summary>
    /// <param name="key">缓存键</param>
    public override TimeSpan GetListTtl([NotNull] string key) => TimeSpan.FromSeconds(RedisClient.Ttl(GetRequiredRedisKey(key)));

    /// <summary>
    /// 从 List 中获取范围列表
    /// </summary>
    /// <param name="key">缓存键</param>
    /// <param name="startingFrom">开始范围</param>
    /// <param name="endingAt">结束范围</param>
    public override List<string> GetRangeFromList([NotNull] string key, int startingFrom, int endingAt) => RedisClient.LRange(GetRequiredRedisKey(key), startingFrom, endingAt).ToList();

    /// <summary>
    /// 从 Set 中获取范围列表
    /// </summary>
    /// <param name="key">缓存键</param>
    /// <param name="startingFrom">开始范围</param>
    /// <param name="endingAt">结束范围</param>
    public override List<string> GetRangeFromSet([NotNull] string key, int startingFrom, int endingAt) => RedisClient.ZRange(GetRequiredRedisKey(key), startingFrom, endingAt).ToList();

    /// <summary>
    /// 获取 Set 计数器
    /// </summary>
    /// <param name="key">缓存键</param>
    public override long GetSetCount([NotNull] string key) => RedisClient.ZCard(GetRequiredRedisKey(key));

    /// <summary>
    /// 获取 Set 剩余时间
    /// </summary>
    /// <param name="key">缓存键</param>
    public override TimeSpan GetSetTtl([NotNull] string key) => TimeSpan.FromSeconds(RedisClient.Ttl(GetRequiredRedisKey(key)));

    /// <summary>
    /// 获取状态数据
    /// </summary>
    /// <param name="jobId">作业标识</param>
    public override StateData GetStateData([NotNull] string jobId)
    {
        if (jobId == null)
            throw new ArgumentNullException(nameof(jobId));
        var entries = RedisClient.HGetAll(_storage.GetRedisKey($"job:{jobId}:state"));
        if (entries.Count == 0)
            return null;

        var name = entries["State"];
        entries.TryGetValue("Reason", out var reason);
        entries.Remove("State");
        entries.Remove("Reason");

        return new StateData
        {
            Name = name,
            Reason = reason,
            Data = entries
        };
    }

    /// <summary>
    /// 从 Hash 中获取值
    /// </summary>
    /// <param name="key">缓存键</param>
    /// <param name="name">名称</param>
    public override string GetValueFromHash([NotNull] string key, [NotNull] string name)
    {
        if (name == null)
            throw new ArgumentNullException(nameof(name));
        return RedisClient.HGet(GetRequiredRedisKey(key), name);
    }

    /// <summary>
    /// 获取 Redis 服务端 UTC 时间
    /// </summary>
    public override DateTime GetUtcDateTime()
    {
        var serverTimes = RedisClient.NodesServerManager.Time();
        if (serverTimes == null || serverTimes.Length == 0)
            throw new InvalidOperationException("Unable to retrieve Redis server time.");
        return serverTimes[0].Item2;
    }

    /// <summary>
    /// 心跳包
    /// </summary>
    /// <param name="serverId">服务器标识</param>
    public override void Heartbeat([NotNull] string serverId)
    {
        if (serverId == null)
            throw new ArgumentNullException(nameof(serverId));
        RedisClient.HSet(_storage.GetRedisKey($"server:{serverId}"), "Heartbeat", JobHelper.SerializeDateTime(DateTime.UtcNow));
    }

    /// <summary>
    /// 移除服务器
    /// </summary>
    /// <param name="serverId">服务器标识</param>
    public override void RemoveServer([NotNull] string serverId)
    {
        if (serverId == null)
            throw new ArgumentNullException(nameof(serverId));
        RedisClient.StartPipe()
            .SRem(_storage.GetRedisKey("servers"), serverId)
            .Del(_storage.GetRedisKey($"server:{serverId}"))
            .Del(_storage.GetRedisKey($"server:{serverId}:queues"))
            .EndPipe();
    }

    /// <summary>
    /// 移除超时的服务器列表
    /// </summary>
    /// <param name="timeOut">超时时间</param>
    public override int RemoveTimedOutServers(TimeSpan timeOut)
    {
        var serverNames = RedisClient.SMembers(_storage.GetRedisKey("servers"));
        var heartbeats = new Dictionary<string, Tuple<DateTime, DateTime?>>();
        var utcNow = DateTime.UtcNow;

        foreach (var serverName in serverNames)
        {
            var srv = RedisClient.HMGet(_storage.GetRedisKey($"server:{serverName}"), new[] { "StartedAt", "Heartbeat" });
            heartbeats.Add(serverName, new Tuple<DateTime, DateTime?>(JobHelper.DeserializeDateTime(srv[0]), JobHelper.DeserializeNullableDateTime(srv[1])));
        }

        var removedServerCount = 0;
        foreach (var heartbeat in heartbeats)
        {
            var maxTime = new DateTime(Math.Max(heartbeat.Value.Item1.Ticks, (heartbeat.Value.Item2 ?? DateTime.MinValue).Ticks));
            if (utcNow > maxTime.Add(timeOut))
            {
                RemoveServer(heartbeat.Key);
                removedServerCount++;
            }
        }
        return removedServerCount;
    }

    /// <summary>
    /// 设置作业参数
    /// </summary>
    /// <param name="jobId">作业标识</param>
    /// <param name="name">名称</param>
    /// <param name="value">值</param>
    public override void SetJobParameter([NotNull] string jobId, [NotNull] string name, string value)
    {
        if (jobId == null)
            throw new ArgumentNullException(nameof(jobId));
        if (name == null)
            throw new ArgumentNullException(nameof(name));
        RedisClient.HSet(_storage.GetRedisKey($"job:{jobId}"), name, value);
    }

    /// <summary>
    /// 设置范围列表到 Hash
    /// </summary>
    /// <param name="key">缓存键</param>
    /// <param name="keyValuePairs">键值对集合</param>
    public override void SetRangeInHash([NotNull] string key, [NotNull] IEnumerable<KeyValuePair<string, string>> keyValuePairs)
    {
        if (keyValuePairs == null)
            throw new ArgumentNullException(nameof(keyValuePairs));
        RedisClient.HMSet(GetRequiredRedisKey(key), keyValuePairs.DicToObjectArray());
    }

    private string GetRequiredRedisKey(string key)
    {
        if (key == null)
            throw new ArgumentNullException(nameof(key));

        return _storage.GetRedisKey(key);
    }

    private string GetRequiredRedisKey(string key, string paramName)
    {
        if (key == null)
            throw new ArgumentNullException(paramName);

        return _storage.GetRedisKey(key);
    }
}