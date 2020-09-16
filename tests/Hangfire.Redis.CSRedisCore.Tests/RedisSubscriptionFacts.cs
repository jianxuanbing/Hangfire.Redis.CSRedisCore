using System;
using System.Diagnostics;
using System.Threading;
using Xunit;

namespace Hangfire.Redis.Tests
{
    [CleanRedis]
    public class RedisSubscriptionFacts
    {
        private readonly CancellationTokenSource _cts;
        private readonly RedisStorage _storage;

        public RedisSubscriptionFacts()
        {
            _cts = new CancellationTokenSource();

            var options = new RedisStorageOptions() { };
            _storage = new RedisStorage(RedisUtils.RedisClient, options);

        }

        [Fact]
        public void Ctor_ThrowAnException_WhenStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>("storage",
                () => new RedisSubscription(null, RedisUtils.RedisClient));
        }

        [Fact]
        public void Ctor_ThrowAnException_WhenSubscriberIsNull()
        {
            Assert.Throws<ArgumentNullException>("redisClient",
                () => new RedisSubscription(_storage, null));
        }
        [Fact]
        public void WaitForJob_WaitForTheTimeout()
        {
            //Arrange
            Stopwatch sw = new Stopwatch();
            var subscription = new RedisSubscription(_storage, RedisUtils.RedisClient);
            var timeout = TimeSpan.FromMilliseconds(100);
            sw.Start();

            //Act
            subscription.WaitForJob(timeout, _cts.Token);

            //Assert
            sw.Stop();
            Assert.InRange(sw.ElapsedMilliseconds, 99, 120);
        }
    }
}
