using System;
using System.Threading;
using Hangfire.Common;
using Hangfire.Server;
using Xunit;

namespace Hangfire.Redis.Tests
{
    [CleanRedis]
    public class ExpiredJobsWatcherFacts
    {
        private static readonly TimeSpan CheckInterval = TimeSpan.FromSeconds(1);

        private readonly RedisStorage _storage;
        private readonly CancellationTokenSource _cts;

        public ExpiredJobsWatcherFacts()
        {
            var options = new RedisStorageOptions() { };
            _storage = new RedisStorage(RedisUtils.RedisClient, options);
            _cts = new CancellationTokenSource();
            _cts.Cancel();
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>("storage",
                () => new ExpiredJobsWatcher(null, CheckInterval));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenCheckIntervalIsZero()
        {
            Assert.Throws<ArgumentOutOfRangeException>("checkInterval",
                () => new ExpiredJobsWatcher(_storage, TimeSpan.Zero));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenCheckIntervalIsNegative()
        {
            Assert.Throws<ArgumentOutOfRangeException>("checkInterval",
                () => new ExpiredJobsWatcher(_storage, TimeSpan.FromSeconds(-1)));
        }

        [Fact, CleanRedis]
        public void Execute_DeletesNonExistingJobs()
        {
            var redis = RedisUtils.RedisClient;

            Assert.Equal(0, redis.LLen("{hangfire}:succeeded"));
            Assert.Equal(0, redis.LLen("{hangfire}:deleted"));

            // Arrange
            redis.RPush("{hangfire}:succeded", "my-job");
            redis.RPush("{hangfire}:deleted", "other-job");

            var watcher = CreateWatcher();

            // Act
            watcher.Execute(_cts.Token);

            // Assert
            Assert.Equal(0, redis.LLen("{hangfire}:succeeded"));
            Assert.Equal(0, redis.LLen("{hangfire}:deleted"));
        }

        [Fact, CleanRedis]
        public void Execute_DoesNotDeleteExistingJobs()
        {
            var redis = RedisUtils.RedisClient;
            // Arrange
            redis.RPush("{hangfire}:succeeded", "my-job");
            redis.HSet("{hangfire}:job:my-job", "Fetched",
                JobHelper.SerializeDateTime(DateTime.UtcNow.AddDays(-1)));

            redis.RPush("{hangfire}:deleted", "other-job");
            redis.HSet("{hangfire}:job:other-job", "Fetched",
                JobHelper.SerializeDateTime(DateTime.UtcNow.AddDays(-1)));

            var watcher = CreateWatcher();

            // Act
            watcher.Execute(_cts.Token);

            // Assert
            Assert.Equal(1, redis.LLen("{hangfire}:succeeded"));
            Assert.Equal(1, redis.LLen("{hangfire}:deleted"));
        }

        private IServerComponent CreateWatcher() => new ExpiredJobsWatcher(_storage, CheckInterval);
    }
}
