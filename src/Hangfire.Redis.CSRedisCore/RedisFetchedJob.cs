using System;
using CSRedis;
using Hangfire.Annotations;
using Hangfire.Storage;

namespace Hangfire.Redis;

/// <summary>
/// Redis拉取作业
/// </summary>
internal class RedisFetchedJob : IFetchedJob
{
    private const string RemoveFromFetchedListScript = @"
local dequeuedKey = KEYS[1]
local queueMarker = string.find(dequeuedKey, 'queue:', 1, true)
if not queueMarker then
    return redis.error_reply('Unexpected fetched queue key: ' .. dequeuedKey)
end

local prefix = string.sub(dequeuedKey, 1, queueMarker - 1)
redis.call('LREM', KEYS[1], -1, ARGV[1])
redis.call('HDEL', prefix .. 'job:' .. ARGV[1], 'Fetched', 'Checked')
return 1";

    private const string RequeueScript = @"
local queueKey = KEYS[1]
local queueMarker = string.find(queueKey, 'queue:', 1, true)
if not queueMarker then
    return redis.error_reply('Unexpected queue key: ' .. queueKey)
end

local prefix = string.sub(queueKey, 1, queueMarker - 1)
local dequeuedKey = queueKey .. ':dequeued'
redis.call('RPUSH', KEYS[1], ARGV[1])
redis.call('PUBLISH', ARGV[2], ARGV[1])
redis.call('LREM', dequeuedKey, -1, ARGV[1])
redis.call('HDEL', prefix .. 'job:' .. ARGV[1], 'Fetched', 'Checked')
return 1";

    /// <summary>
    /// Redis存储
    /// </summary>
    private readonly RedisStorage _storage;

    /// <summary>
    /// Redis客户端
    /// </summary>
    private readonly CSRedisClient _redisClient;

    /// <summary>
    /// 是否已释放
    /// </summary>
    private bool _disposed;

    /// <summary>
    /// 是否已从队列移除
    /// </summary>
    private bool _removedFromQueue;

    /// <summary>
    /// 是否重新入队
    /// </summary>
    private bool _requeued;

    /// <summary>
    /// 初始化一个<see cref="RedisFetchedJob"/>类型的实例
    /// </summary>
    /// <param name="storage">Redis存储</param>
    /// <param name="redisClient">Redis客户端</param>
    /// <param name="jobId">作业标识</param>
    /// <param name="queue">队列</param>
    public RedisFetchedJob([NotNull] RedisStorage storage
        , [NotNull] CSRedisClient redisClient
        , [NotNull] string jobId
        , [NotNull] string queue)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _redisClient = redisClient ?? throw new ArgumentNullException(nameof(redisClient));
        JobId = jobId ?? throw new ArgumentNullException(nameof(jobId));
        Queue = queue ?? throw new ArgumentNullException(nameof(queue));
    }

    /// <summary>
    /// 作业标识
    /// </summary>
    public string JobId { get; }

    /// <summary>
    /// 队列
    /// </summary>
    public string Queue { get; }

    internal bool IsCompleted => _removedFromQueue || _requeued;

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
            return;
        if (!_removedFromQueue && !_requeued)
            Requeue();
        _disposed = true;
    }

    /// <summary>
    /// 从队列中移除
    /// </summary>
    public void RemoveFromQueue()
    {
        if (_removedFromQueue || _requeued)
            return;

        ExecuteRemoveFromFetchedList(_redisClient, _storage, Queue, JobId);
        _removedFromQueue = true;
    }

    /// <summary>
    /// 重新入队
    /// </summary>
    public void Requeue()
    {
        if (_requeued || _removedFromQueue)
            return;

        ExecuteRequeue(_redisClient, _storage, Queue, JobId);
        _requeued = true;
    }

    internal void MarkAsRemovedFromQueue()
    {
        _removedFromQueue = true;
    }

    internal static void ScheduleRemoveFromFetchedList(CSRedisClientPipe<string> redisClientPipe, RedisStorage storage, string queue, string jobId)
    {
        if (redisClientPipe == null)
            throw new ArgumentNullException(nameof(redisClientPipe));

        redisClientPipe.Eval(
            RemoveFromFetchedListScript,
            storage.GetRedisKey($"queue:{queue}:dequeued"),
            jobId);
    }

    internal static void ExecuteRemoveFromFetchedList(CSRedisClient redisClient, RedisStorage storage, string queue, string jobId)
    {
        if (redisClient == null)
            throw new ArgumentNullException(nameof(redisClient));

        redisClient.Eval(
            RemoveFromFetchedListScript,
            storage.GetRedisKey($"queue:{queue}:dequeued"),
            jobId);
    }

    internal static void ExecuteRequeue(CSRedisClient redisClient, RedisStorage storage, string queue, string jobId)
    {
        if (redisClient == null)
            throw new ArgumentNullException(nameof(redisClient));

        redisClient.Eval(
            RequeueScript,
            storage.GetRedisKey($"queue:{queue}"),
            jobId,
            storage.SubscriptionChannel);
    }
}