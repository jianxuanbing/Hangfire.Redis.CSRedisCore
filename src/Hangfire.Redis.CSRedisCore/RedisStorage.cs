using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using CSRedis;
using Hangfire.Annotations;
using Hangfire.Dashboard;
using Hangfire.Logging;
using Hangfire.Redis.States;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Redis;

/// <summary>
/// 基于CSRedisCore实现的Redis存储
/// </summary>
public class RedisStorage : JobStorage, IDisposable
{
    private static readonly HashSet<string> SupportedFeatures = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        JobStorageFeatures.ExtendedApi,
        JobStorageFeatures.Connection.BatchedGetFirstByLowest,
        JobStorageFeatures.Connection.GetUtcDateTime,
        JobStorageFeatures.JobQueueProperty
    };

    /// <summary>
    /// Redis存储选项配置
    /// </summary>
    private readonly RedisStorageOptions _options;

    /// <summary>
    /// 是否拥有 Redis 客户端实例
    /// </summary>
    private readonly bool _ownsRedisClient;

    /// <summary>
    /// Redis订阅
    /// </summary>
    private readonly Lazy<RedisSubscription> _subscription;

    /// <summary>
    /// 订阅通道名称
    /// </summary>
    private readonly string _subscriptionChannel;

    /// <summary>
    /// 是否已释放
    /// </summary>
    private bool _disposed;

    /// <summary>
    /// Redis客户端
    /// </summary>
    public CSRedisClient RedisClient { get; }

    /// <summary>
    /// 初始化一个<see cref="RedisClient"/>类型的实例
    /// </summary>
    public RedisStorage() : this("localhost:6379") { }

    /// <summary>
    /// 初始化一个<see cref="RedisClient"/>类型的实例
    /// </summary>
    /// <param name="connectionString">连接字符串</param>
    /// <param name="options">Redis存储选项配置</param>
    public RedisStorage(string connectionString, RedisStorageOptions options = null)
    {
        if (connectionString == null)
            throw new ArgumentNullException(nameof(connectionString));
        // TODO: 此处需要对连接字符串进行解析
        _options = options ?? new RedisStorageOptions();
        _ownsRedisClient = true;
        RedisClient = new CSRedisClient(connectionString);
        _subscriptionChannel = _options.Prefix + "JobFetchChannel";
        _subscription = CreateSubscriptionFactory();
    }

    /// <summary>
    /// 初始化一个<see cref="RedisClient"/>类型的实例
    /// </summary>
    /// <param name="redisClient">Redis客户端</param>
    /// <param name="options">Redis存储选项配置</param>
    public RedisStorage(CSRedisClient redisClient, RedisStorageOptions options = null)
    {
        RedisClient = redisClient ?? throw new ArgumentNullException(nameof(redisClient));
        _options = options ?? new RedisStorageOptions();
        _ownsRedisClient = false;
        _subscriptionChannel = _options.Prefix + "JobFetchChannel";
        _subscription = CreateSubscriptionFactory();
    }

    /// <summary>
    /// 已成功列表大小
    /// </summary>
    internal int SucceededListSize => _options.SucceededListSize;

    /// <summary>
    /// 已删除列表大小
    /// </summary>
    internal int DeletedListSize => _options.DeletedListSize;

    /// <summary>
    /// 订阅管道
    /// </summary>
    internal string SubscriptionChannel => _subscriptionChannel;

    /// <summary>
    /// LIFO(后进先出)队列
    /// </summary>
    internal string[] LifoQueues => _options.LifoQueues;

    /// <summary>
    /// 获取监控API
    /// </summary>
    public override IMonitoringApi GetMonitoringApi()
    {
        EnsureNotDisposed();
        return new RedisMonitoringApi(this, RedisClient);
    }

    /// <summary>
    /// 获取存储特性
    /// </summary>
    /// <param name="featureId">特性标识</param>
    public override bool HasFeature([NotNull] string featureId)
    {
        if (featureId == null)
            throw new ArgumentNullException(nameof(featureId));
        return SupportedFeatures.Contains(featureId) || base.HasFeature(featureId);
    }

    /// <summary>
    /// 获取存储连接
    /// </summary>
    public override IStorageConnection GetConnection()
    {
        EnsureNotDisposed();
        return new RedisConnection(this, RedisClient, _subscription.Value, _options.FetchTimeout);
    }

    /// <summary>
    /// 获取组件集合
    /// </summary>
#pragma warning disable CS0618, CS0672
    public override IEnumerable<IServerComponent> GetComponents()
    {
        yield return new FetchedJobsWatcher(this, _options.InvisibilityTimeout);
        yield return new ExpiredJobsWatcher(this, _options.ExpiryCheckInterval);
        yield return _subscription.Value;
    }
#pragma warning restore CS0618, CS0672

    /// <summary>
    /// 获取状态处理器集合
    /// </summary>
    public override IEnumerable<IStateHandler> GetStateHandlers()
    {
        yield return new FailedStateHandler();
        yield return new ProcessingStateHandler();
        yield return new SucceededStateHandler();
        yield return new DeletedStateHandler();
    }

    /// <summary>
    /// 将选项配置输出到日志
    /// </summary>
    /// <param name="logger">日志</param>
    public override void WriteOptionsToLog(ILog logger)
    {
        logger.Debug("Using the following options for Redis job storage:");
    }

    /// <summary>
    /// 输出字符串
    /// </summary>
    public override string ToString() => RedisClient.ToString();

    /// <summary>
    /// 释放当前存储实例持有的订阅与 Redis 客户端资源。
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
            return;

        if (_subscription.IsValueCreated)
            _subscription.Value.Dispose();

        if (_ownsRedisClient)
            RedisClient.Dispose();

        _disposed = true;
        GC.SuppressFinalize(this);
    }

    private void EnsureNotDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(RedisStorage));
    }

    private Lazy<RedisSubscription> CreateSubscriptionFactory() =>
        new Lazy<RedisSubscription>(() => new RedisSubscription(this, RedisClient), LazyThreadSafetyMode.ExecutionAndPublication);

    /// <summary>
    /// 获取Redis缓存键
    /// </summary>
    /// <param name="key">缓存键</param>
    internal string GetRedisKey([NotNull] string key)
    {
        if (key == null)
            throw new ArgumentNullException(nameof(key));
        return _options.Prefix + key;
    }

    /// <summary>
    /// 获取仪表板指标
    /// </summary>
    /// <param name="title">标题</param>
    /// <param name="key">缓存键</param>
    public static DashboardMetric GetDashboardMetricFromRedisInfo(string title, string key)
    {
        return new DashboardMetric($"redis:{key}", title, (razorPage) =>
        {
            using (var redisCnn = razorPage.Storage.GetConnection())
            {
                var rawInfo = (redisCnn as RedisConnection).RedisClient.NodesServerManager.Info().ToDictionary(r => r.node, r => r.value);
                return new Metric(rawInfo[key]);
            }
        });
    }
}