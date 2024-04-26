using System;
using System.Collections.Generic;
using System.Linq;
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
public class RedisStorage : JobStorage
{
    /// <summary>
    /// Redis存储选项配置
    /// </summary>
    private readonly RedisStorageOptions _options;

    /// <summary>
    /// Redis订阅
    /// </summary>
    private readonly RedisSubscription _subscription;

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
        RedisClient = new CSRedisClient(connectionString);
        _subscription = new RedisSubscription(this, RedisClient);
    }

    /// <summary>
    /// 初始化一个<see cref="RedisClient"/>类型的实例
    /// </summary>
    /// <param name="redisClient">Redis客户端</param>
    /// <param name="options">Redis存储选项配置</param>
    public RedisStorage(CSRedisClient redisClient, RedisStorageOptions options = null)
    {
        RedisClient = redisClient;
        _options = options ?? new RedisStorageOptions();
        _subscription = new RedisSubscription(this, redisClient);
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
    internal string SubscriptionChannel => _subscription.Channel;

    /// <summary>
    /// LIFO(后进先出)队列
    /// </summary>
    internal string[] LifoQueues => _options.LifoQueues;

    /// <summary>
    /// 获取监控API
    /// </summary>
    public override IMonitoringApi GetMonitoringApi() => new RedisMonitoringApi(this, RedisClient);

    /// <summary>
    /// 获取存储连接
    /// </summary>
    public override IStorageConnection GetConnection() => new RedisConnection(this, RedisClient, _subscription, _options.FetchTimeout);

    /// <summary>
    /// 获取组件集合
    /// </summary>
    public override IEnumerable<IServerComponent> GetComponents()
    {
        yield return new FetchedJobsWatcher(this, _options.InvisibilityTimeout);
        yield return new ExpiredJobsWatcher(this, _options.ExpiryCheckInterval);
        yield return _subscription;
    }

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