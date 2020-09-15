using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Logging;
using Hangfire.Server;

namespace Hangfire.Redis
{
    /// <summary>
    /// 已过期作业观察者
    /// </summary>
    internal class ExpiredJobsWatcher : IServerComponent
    {
        /// <summary>
        /// 日志
        /// </summary>
        private static readonly ILog Logger = LogProvider.For<ExpiredJobsWatcher>();

        /// <summary>
        /// Redis存储
        /// </summary>
        private readonly RedisStorage _storage;

        /// <summary>
        /// 检查间隔
        /// </summary>
        private readonly TimeSpan _checkInterval;

        /// <summary>
        /// 已处理的键数组
        /// </summary>
        private static readonly string[] ProcessedKeys =
        {
            Const.Succeeded,
            Const.Deleted,
        };

        /// <summary>
        /// 初始化一个<see cref="ExpiredJobsWatcher"/>类型的实例
        /// </summary>
        /// <param name="storage">Redis存储</param>
        /// <param name="checkInterval">检查间隔</param>
        public ExpiredJobsWatcher(RedisStorage storage, TimeSpan checkInterval)
        {
            if (checkInterval.Ticks < 0)
                throw new ArgumentOutOfRangeException(nameof(checkInterval), "Check interval should be positive.");
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _checkInterval = checkInterval;
        }

        /// <summary>
        /// 执行
        /// </summary>
        /// <param name="cancellationToken">取消令牌</param>
        public void Execute(CancellationToken cancellationToken)
        {
            using (var connection = (RedisConnection)_storage.GetConnection())
            {
                var redis = connection.RedisClient;
                foreach (var key in ProcessedKeys)
                {
                    var redisKey = _storage.GetRedisKey(key);
                    var count = redis.LLen(redisKey);
                    if (count == 0)
                        continue;

                    Logger.InfoFormat("Removing expired records from the '{0}' list...", key);
                    const int batchSize = 100;
                    var keysToRemove = new List<string>();
                    for (var last = count - 1; last >= 0; last -= batchSize)
                    {
                        var first = Math.Max(0, last - batchSize + 1);
                        var jobIds = redis.LRange(redisKey, first, last);
                        if (jobIds.Length == 0)
                            continue;
                        var tasks = new Task[jobIds.Length];
                        for (var i = 0; i < jobIds.Length; i++)
                            tasks[i] = redis.ExistsAsync(_storage.GetRedisKey($"job:{jobIds[i]}"));
                        Task.WaitAll(tasks);
                        keysToRemove.AddRange(jobIds.Where((t, i) => !((Task<bool>)tasks[i]).Result));
                    }

                    if (keysToRemove.Count == 0)
                        continue;

                    Logger.InfoFormat("Removing {0} expired jobs from '{1}' list...", keysToRemove.Count, key);
                    using (var transaction = connection.CreateWriteTransaction())
                    {
                        foreach (var jobId in keysToRemove)
                            transaction.RemoveFromList(key, jobId);
                        transaction.Commit();
                    }
                }
            }
            cancellationToken.WaitHandle.WaitOne(_checkInterval);
        }

        /// <summary>
        /// 输出字符串
        /// </summary>
        public override string ToString() => GetType().ToString();
    }
}
