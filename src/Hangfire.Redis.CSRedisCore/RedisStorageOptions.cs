using System;

namespace Hangfire.Redis
{
    /// <summary>
    /// Redis存储选项配置
    /// </summary>
    public class RedisStorageOptions
    {
        /// <summary>
        /// 默认前缀
        /// </summary>
        public const string DefaultPrefix = "{hangfire}:";

        /// <summary>
        /// 初始化一个<see cref="RedisStorageOptions"/>类型的实例
        /// </summary>
        public RedisStorageOptions()
        {
            InvisibilityTimeout = TimeSpan.FromMinutes(30);
            FetchTimeout = TimeSpan.FromMinutes(3);
            ExpiryCheckInterval = TimeSpan.FromHours(1);
            Db = 0;
            Prefix = DefaultPrefix;
            SucceededListSize = 499;
            DeletedListSize = 499;
            LifoQueues = new string[0];
        }

        /// <summary>
        /// 隐形超时时间
        /// </summary>
        public TimeSpan InvisibilityTimeout { get; set; }

        /// <summary>
        /// 拉取超时时间
        /// </summary>
        public TimeSpan FetchTimeout { get; set; }

        /// <summary>
        /// 到期时间检查间隔
        /// </summary>
        public TimeSpan ExpiryCheckInterval { get; set; }

        /// <summary>
        /// 缓存键前缀
        /// </summary>
        public string Prefix { get; set; }

        /// <summary>
        /// 数据库
        /// </summary>
        public int Db { get; set; }

        /// <summary>
        /// 已成功列表大小
        /// </summary>
        public int SucceededListSize { get; set; }

        /// <summary>
        /// 已删除列表大小
        /// </summary>
        public int DeletedListSize { get; set; }

        /// <summary>
        /// Lifo(后进先出)队列
        /// </summary>
        public string[] LifoQueues { get; set; }
    }
}
