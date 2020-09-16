using Xunit;

namespace Hangfire.Redis.Tests
{
    public class RedisTest
    {
        private readonly CSRedis.CSRedisClient _redis;

        public RedisTest()
        {
            _redis = RedisUtils.RedisClient;
        }


        [Fact, CleanRedis]
        public void RedisSampleTest()
        {
            var defaultValue = _redis.Get("samplekey");
            Assert.True(string.IsNullOrWhiteSpace(defaultValue));
        }
    }
}
