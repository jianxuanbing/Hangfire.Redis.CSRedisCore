using System;
using CSRedis;
using Hangfire.Annotations;

namespace Hangfire.Redis
{
    /// <summary>
    /// Redis存储扩展
    /// </summary>
    public static class RedisStorageExtensions
    {
        /// <summary>
        /// 启用基于CSRedisCore实现的Redis存储
        /// </summary>
        /// <param name="configuration">Hangfire全局配置</param>
        public static IGlobalConfiguration<RedisStorage> UseRedisStorage([NotNull] this IGlobalConfiguration configuration)
        {
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));
            var storage=new RedisStorage();
            return configuration.UseStorage(storage);
        }

        /// <summary>
        /// 启用基于CSRedisCore实现的Redis存储
        /// </summary>
        /// <param name="configuration">Hangfire全局配置</param>
        /// <param name="redisClient">CSRedisCore客户端</param>
        /// <param name="options">Redis存储选项配置</param>
        public static IGlobalConfiguration<RedisStorage> UseRedisStorage([NotNull] this IGlobalConfiguration configuration, [NotNull] CSRedisClient redisClient, RedisStorageOptions options = null)
        {
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));
            if(redisClient==null)
                throw new ArgumentNullException(nameof(redisClient));
            var storage = new RedisStorage(redisClient, options);
            return configuration.UseStorage(storage);
        }

        /// <summary>
        /// 启用基于CSRedisCore实现的Redis存储
        /// </summary>
        /// <param name="configuration">Hangfire全局配置</param>
        /// <param name="nameOrConnectionString">连接字符串</param>
        /// <param name="options">Redis存储选项配置</param>
        public static IGlobalConfiguration<RedisStorage> UseRedisStorage([NotNull] this IGlobalConfiguration configuration, [NotNull] string nameOrConnectionString, RedisStorageOptions options = null)
        {
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));
            if (nameOrConnectionString == null)
                throw new ArgumentNullException(nameof(nameOrConnectionString));
            var storage = new RedisStorage(nameOrConnectionString, options);
            return configuration.UseStorage(storage);
        }
    }
}
