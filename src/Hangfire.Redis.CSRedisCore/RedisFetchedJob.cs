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
        RemoveFromFetchedList();
        _removedFromQueue = true;
    }

    /// <summary>
    /// 从已拉取列表中移除
    /// </summary>
    private void RemoveFromFetchedList()
    {
        _redisClient.StartPipe()
            .LRem(_storage.GetRedisKey($"queue:{Queue}:dequeued"), -1, JobId)
            .HDel(_storage.GetRedisKey($"job:{JobId}"), new[] { "Fetched", "Checked" })
            .EndPipe();
    }

    /// <summary>
    /// 重新入队
    /// </summary>
    public void Requeue()
    {
        _redisClient.RPush(_storage.GetRedisKey($"queue:{Queue}"), JobId);
        RemoveFromFetchedList();
        _requeued = true;
    }
}