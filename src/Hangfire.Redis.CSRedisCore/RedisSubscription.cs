using System;
using System.Threading;
using CSRedis;
using Hangfire.Server;
using static CSRedis.CSRedisClient;

namespace Hangfire.Redis;

/// <summary>
/// Redis订阅
/// </summary>
#pragma warning disable CS0618
internal class RedisSubscription : IServerComponent, IDisposable
{
    /// <summary>
    /// 手动重置事件
    /// </summary>
    private readonly ManualResetEvent _mre = new ManualResetEvent(false);

    /// <summary>
    /// Redis存储
    /// </summary>
    private readonly RedisStorage _storage;

    /// <summary>
    /// Redis客户端
    /// </summary>
    private readonly CSRedisClient _redisClient;

    /// <summary>
    /// 同步锁
    /// </summary>
    private readonly object _syncRoot = new object();

    /// <summary>
    /// 订阅对象
    /// </summary>
    private SubscribeObject _subscribeObject;

    /// <summary>
    /// 是否已释放
    /// </summary>
    private bool _disposed;

    /// <summary>
    /// 初始化一个<see cref="RedisSubscription"/>类型的实例
    /// </summary>
    /// <param name="storage">Redis存储</param>
    /// <param name="redisClient">Redis客户端</param>
    public RedisSubscription(RedisStorage storage, CSRedisClient redisClient)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _redisClient = redisClient ?? throw new ArgumentNullException(nameof(redisClient));
        Channel = _storage.GetRedisKey("JobFetchChannel");
    }

    /// <summary>
    /// 管道
    /// </summary>
    public string Channel { get; }

    /// <summary>
    /// 执行
    /// </summary>
    /// <param name="cancellationToken">取消令牌</param>
    public void Execute(CancellationToken cancellationToken)
    {
        EnsureSubscribed();
        cancellationToken.WaitHandle.WaitOne();
        if (cancellationToken.IsCancellationRequested)
            Dispose();
    }

    /// <summary>
    /// 等待作业
    /// </summary>
    /// <param name="timeout">超时时间</param>
    /// <param name="cancellationToken">取消令牌</param>
    public void WaitForJob(TimeSpan timeout, CancellationToken cancellationToken)
    {
        EnsureSubscribed();
        _mre.Reset();
        WaitHandle.WaitAny(new[] {_mre, cancellationToken.WaitHandle}, timeout);
    }

    public void Dispose()
    {
        lock (_syncRoot)
        {
            if (_disposed)
                return;

            _subscribeObject?.Unsubscribe();
            _subscribeObject?.Dispose();
            _mre.Dispose();
            _disposed = true;
        }
    }

    private void EnsureSubscribed()
    {
        lock (_syncRoot)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(RedisSubscription));

            _subscribeObject ??= _redisClient.Subscribe((Channel, r => _mre.Set()));
        }
    }
}
#pragma warning restore CS0618