using System;
using System.Collections.Generic;
using System.Linq;
using CSRedis;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Redis;

/// <summary>
/// Redis只写事务
/// </summary>
internal class RedisWriteOnlyTransaction : JobStorageTransaction
{
    private const string CreateJobScript = @"
redis.call('HMSET', KEYS[1], unpack(ARGV, 2))
redis.call('PEXPIRE', KEYS[1], ARGV[1])
return 1";

    /// <summary>
    /// Redis存储
    /// </summary>
    private readonly RedisStorage _storage;

    /// <summary>
    /// Redis客户端管道
    /// </summary>
    private readonly CSRedisClientPipe<string> _redisClientPipe;

    /// <summary>
    /// 事务内已确认的拉取作业
    /// </summary>
    private readonly HashSet<RedisFetchedJob> _removedFetchedJobs = new HashSet<RedisFetchedJob>();

    /// <summary>
    /// 是否已提交
    /// </summary>
    private bool _committed;

    /// <summary>
    /// 初始化一个<see cref="RedisWriteOnlyTransaction"/>类型的实例
    /// </summary>
    /// <param name="storage">Redis存储</param>
    public RedisWriteOnlyTransaction(RedisStorage storage)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _redisClientPipe = _storage.RedisClient.StartPipe();
    }

    /// <summary>
    /// 添加列表到 Set
    /// </summary>
    /// <param name="key">键</param>
    /// <param name="items">列表</param>
    public override void AddRangeToSet([NotNull]string key, [NotNull] IList<string> items) => _redisClientPipe.ZAdd(GetRequiredRedisKey(key), items.Select(x => (0M, (object) x)).ToArray());

    /// <summary>
    /// 设置 Hash 过期时间
    /// </summary>
    /// <param name="key">缓存键</param>
    /// <param name="expireIn">过期时间</param>
    public override void ExpireHash([NotNull] string key, TimeSpan expireIn) => _redisClientPipe.Expire(GetRequiredRedisKey(key), expireIn);

    /// <summary>
    /// 设置 List 过期时间
    /// </summary>
    /// <param name="key">缓存键</param>
    /// <param name="expireIn">过期时间</param>
    public override void ExpireList([NotNull] string key, TimeSpan expireIn) => _redisClientPipe.Expire(GetRequiredRedisKey(key), expireIn);

    /// <summary>
    /// 设置 Set 过期时间
    /// </summary>
    /// <param name="key">缓存键</param>
    /// <param name="expireIn">过期时间</param>
    public override void ExpireSet([NotNull] string key, TimeSpan expireIn) => _redisClientPipe.Expire(GetRequiredRedisKey(key), expireIn);

    /// <summary>
    /// 设置 Hash 持久保持
    /// </summary>
    /// <param name="key">缓存键</param>
    public override void PersistHash([NotNull] string key) => _redisClientPipe.Persist(GetRequiredRedisKey(key));

    /// <summary>
    /// 设置 List 持久保持
    /// </summary>
    /// <param name="key">缓存键</param>
    public override void PersistList([NotNull] string key) => _redisClientPipe.Persist(GetRequiredRedisKey(key));

    /// <summary>
    /// 设置 Set 持久保持
    /// </summary>
    /// <param name="key">缓存键</param>
    public override void PersistSet([NotNull] string key) => _redisClientPipe.Persist(GetRequiredRedisKey(key));

    /// <summary>
    /// 移除 Set
    /// </summary>
    /// <param name="key">缓存键</param>
    public override void RemoveSet([NotNull] string key) => _redisClientPipe.Del(GetRequiredRedisKey(key));

    /// <summary>
    /// 提交
    /// </summary>
    public override void Commit()
    {
        _redisClientPipe.EndPipe();

        foreach (var fetchedJob in _removedFetchedJobs)
            fetchedJob.MarkAsRemovedFromQueue();

        _committed = true;
    }

    /// <summary>
    /// 设置 作业 过期时间
    /// </summary>
    /// <param name="jobId">作业标识</param>
    /// <param name="expireIn">过期时间</param>
    public override void ExpireJob([NotNull] string jobId, TimeSpan expireIn)
    {
        if (jobId == null) 
            throw new ArgumentNullException(nameof(jobId));
        _redisClientPipe.Expire(_storage.GetRedisKey($"job:{jobId}"), expireIn);
        _redisClientPipe.Expire(_storage.GetRedisKey($"job:{jobId}:history"), expireIn);
        _redisClientPipe.Expire(_storage.GetRedisKey($"job:{jobId}:state"), expireIn);
    }

    /// <summary>
    /// 设置 作业 持久保持
    /// </summary>
    /// <param name="jobId">作业标识</param>
    public override void PersistJob([NotNull] string jobId)
    {
        if (jobId == null) 
            throw new ArgumentNullException(nameof(jobId));
        _redisClientPipe.Persist(_storage.GetRedisKey($"job:{jobId}"));
        _redisClientPipe.Persist(_storage.GetRedisKey($"job:{jobId}:history"));
        _redisClientPipe.Persist(_storage.GetRedisKey($"job:{jobId}:state"));
    }

    /// <summary>
    /// 设置 作业 状态
    /// </summary>
    /// <param name="jobId">作业标识</param>
    /// <param name="state">状态</param>
    public override void SetJobState([NotNull] string jobId, [NotNull] IState state)
    {
        if (jobId == null) 
            throw new ArgumentNullException(nameof(jobId));
        if (state == null) 
            throw new ArgumentNullException(nameof(state));
        _redisClientPipe.HSet(_storage.GetRedisKey($"job:{jobId}"), "State", state.Name);
        _redisClientPipe.Del(_storage.GetRedisKey($"job:{jobId}:state"));
        var storedData = new Dictionary<string, string>(state.SerializeData())
        {
            { "State", state.Name }
        };

        if (state.Reason != null)
            storedData.Add("Reason", state.Reason);

        _redisClientPipe.HMSet(_storage.GetRedisKey($"job:{jobId}:state"), storedData.DicToObjectArray());
        AddJobState(jobId, state);
    }

    /// <summary>
    /// 添加 作业 状态
    /// </summary>
    /// <param name="jobId">作业标识</param>
    /// <param name="state">状态</param>
    public override void AddJobState([NotNull] string jobId, [NotNull] IState state)
    {
        if (jobId == null)
            throw new ArgumentNullException(nameof(jobId));
        if (state == null)
            throw new ArgumentNullException(nameof(state));
        var storedData = new Dictionary<string, string>(state.SerializeData())
        {
            { "State", state.Name },
            { "Reason", state.Reason },
            { "CreatedAt", JobHelper.SerializeDateTime(DateTime.UtcNow) }
        };

        _redisClientPipe.RPush(_storage.GetRedisKey($"job:{jobId}:history"), SerializationHelper.Serialize(storedData));
    }

    /// <summary>
    /// 添加到队列
    /// </summary>
    /// <param name="queue">队列</param>
    /// <param name="jobId">作业标识</param>
    public override void AddToQueue([NotNull] string queue, [NotNull] string jobId)
    {
        if (queue == null) 
            throw new ArgumentNullException(nameof(queue));
        if (jobId == null) 
            throw new ArgumentNullException(nameof(jobId));
        _redisClientPipe.SAdd(_storage.GetRedisKey("queues"), queue);
        if (_storage.LifoQueues != null && _storage.LifoQueues.Contains(queue, StringComparer.OrdinalIgnoreCase))
            _redisClientPipe.RPush(_storage.GetRedisKey($"queue:{queue}"), jobId);
        else
            _redisClientPipe.LPush(_storage.GetRedisKey($"queue:{queue}"), jobId);
        _redisClientPipe.Publish(_storage.SubscriptionChannel, jobId);
    }

    public override void RemoveFromQueue(IFetchedJob fetchedJob)
    {
        if (fetchedJob == null)
            throw new ArgumentNullException(nameof(fetchedJob));

        if (fetchedJob is not RedisFetchedJob redisFetchedJob)
            throw new ArgumentException($"Only {nameof(RedisFetchedJob)} is supported.", nameof(fetchedJob));

        if (redisFetchedJob.IsCompleted || !_removedFetchedJobs.Add(redisFetchedJob))
            return;

        RedisFetchedJob.ScheduleRemoveFromFetchedList(_redisClientPipe, _storage, redisFetchedJob.Queue, redisFetchedJob.JobId);
    }

    public override void SetJobParameter(string jobId, string name, string value)
    {
        if (jobId == null)
            throw new ArgumentNullException(nameof(jobId));
        if (name == null)
            throw new ArgumentNullException(nameof(name));

        _redisClientPipe.HSet(_storage.GetRedisKey($"job:{jobId}"), name, value);
    }

    public override string CreateJob(Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
    {
        if (job == null)
            throw new ArgumentNullException(nameof(job));
        if (parameters == null)
            throw new ArgumentNullException(nameof(parameters));

        var jobId = Guid.NewGuid().ToString("n");
        var storedParameters = RedisConnection.CreateJobHash(job, parameters, createdAt);
        var jobKey = _storage.GetRedisKey($"job:{jobId}");
        var hashEntries = storedParameters.DicToObjectArray();
        var scriptArguments = new object[hashEntries.Length + 1];

        scriptArguments[0] = Math.Max(1L, (long)Math.Ceiling(expireIn.TotalMilliseconds));
        Array.Copy(hashEntries, 0, scriptArguments, 1, hashEntries.Length);

        _redisClientPipe.Eval(CreateJobScript, jobKey, scriptArguments);
        return jobId;
    }

    /// <summary>
    /// 递增计数器
    /// </summary>
    /// <param name="key">缓存键</param>
    public override void IncrementCounter([NotNull] string key) => _redisClientPipe.IncrBy(GetRequiredRedisKey(key));

    /// <summary>
    /// 递增计数器
    /// </summary>
    /// <param name="key">缓存键</param>
    /// <param name="expireIn">过期时间</param>
    public override void IncrementCounter([NotNull] string key, TimeSpan expireIn)
    {
        var redisKey = GetRequiredRedisKey(key);
        _redisClientPipe.IncrBy(redisKey);
        _redisClientPipe.Expire(redisKey, expireIn);
    }

    /// <summary>
    /// 递减计数器
    /// </summary>
    /// <param name="key">缓存键</param>
    public override void DecrementCounter([NotNull] string key) => _redisClientPipe.IncrBy(GetRequiredRedisKey(key), -1);

    /// <summary>
    /// 递减计数器
    /// </summary>
    /// <param name="key">缓存键</param>
    /// <param name="expireIn">过期时间</param>
    public override void DecrementCounter([NotNull] string key, TimeSpan expireIn)
    {
        var redisKey = GetRequiredRedisKey(key);
        _redisClientPipe.IncrBy(redisKey,-1);
        _redisClientPipe.Expire(redisKey, expireIn);
    }

    /// <summary>
    /// 添加到 Set
    /// </summary>
    /// <param name="key">缓存键</param>
    /// <param name="value">值</param>
    public override void AddToSet([NotNull] string key, [NotNull] string value) => AddToSet(key, value, 0);

    /// <summary>
    /// 添加到 Set
    /// </summary>
    /// <param name="key">缓存键</param>
    /// <param name="value">值</param>
    /// <param name="score">分数</param>
    public override void AddToSet([NotNull] string key, [NotNull] string value, double score)
    {
        if (value == null)
            throw new ArgumentNullException(nameof(value));
        _redisClientPipe.ZAdd(GetRequiredRedisKey(key), ((decimal) score, value));
    }

    /// <summary>
    /// 从 Set 中移除
    /// </summary>
    /// <param name="key">缓存键</param>
    /// <param name="value">值</param>
    public override void RemoveFromSet([NotNull] string key, [NotNull] string value)
    {
        if (value == null)
            throw new ArgumentNullException(nameof(value));
        _redisClientPipe.ZRem(GetRequiredRedisKey(key), value);
    }

    /// <summary>
    /// 插入到 List
    /// </summary>
    /// <param name="key">缓存键</param>
    /// <param name="value">值</param>
    public override void InsertToList([NotNull] string key, string value) => _redisClientPipe.LPush(GetRequiredRedisKey(key), value);

    /// <summary>
    /// 从 List 中移除
    /// </summary>
    /// <param name="key">缓存键</param>
    /// <param name="value">值</param>
    public override void RemoveFromList([NotNull] string key, string value) => _redisClientPipe.LRem(GetRequiredRedisKey(key), 0, value);

    /// <summary>
    /// 清空列表
    /// </summary>
    /// <param name="key">缓存键</param>
    /// <param name="keepStartingFrom">开始范围</param>
    /// <param name="keepEndingAt">结束范围</param>
    public override void TrimList([NotNull] string key, int keepStartingFrom, int keepEndingAt) => _redisClientPipe.LTrim(GetRequiredRedisKey(key), keepStartingFrom, keepEndingAt);

    /// <summary>
    /// 设置范围列表到 Hash 
    /// </summary>
    /// <param name="key">缓存键</param>
    /// <param name="keyValuePairs">键值对集合</param>
    public override void SetRangeInHash([NotNull] string key, [NotNull] IEnumerable<KeyValuePair<string, string>> keyValuePairs)
    {
        if (keyValuePairs == null)
            throw new ArgumentNullException(nameof(keyValuePairs));
        _redisClientPipe.HMSet(GetRequiredRedisKey(key), keyValuePairs.DicToObjectArray());
    }

    /// <summary>
    /// 移除 Hash
    /// </summary>
    /// <param name="key">缓存键</param>
    public override void RemoveHash([NotNull] string key) => _redisClientPipe.Del(GetRequiredRedisKey(key));

    /// <summary>
    /// 释放资源
    /// </summary>
    public override void Dispose()
    {
        if (!_committed)
            _removedFetchedJobs.Clear();

        _redisClientPipe.Dispose();
    }

    private string GetRequiredRedisKey(string key)
    {
        if (key == null)
            throw new ArgumentNullException(nameof(key));

        return _storage.GetRedisKey(key);
    }
}