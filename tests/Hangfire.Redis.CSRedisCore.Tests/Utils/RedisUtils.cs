using CSRedis;

// ReSharper disable once CheckNamespace
namespace Hangfire.Redis.Tests
{
    public static class RedisUtils
    {
        public const string ConnectionString = "127.0.0.1:6379,defaultDatabase=1,connectTimeout=30000,poolsize=100";
        public const string ExecutedJobsKey = "{hangfire}:test:executed";

        public static CSRedisClient RedisClient { get; }

        static RedisUtils()
        {
            RedisClient = new CSRedisClient(ConnectionString, new string[] { }, false);
            RedisHelper.Initialization(RedisClient);
        }

        public static void RecordExecution(string queue)
        {
            RedisClient.RPush(ExecutedJobsKey, queue);
        }
    }
}
