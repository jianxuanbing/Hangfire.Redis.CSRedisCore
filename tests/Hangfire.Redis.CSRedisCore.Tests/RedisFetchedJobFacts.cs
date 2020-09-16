using System;
using Xunit;

namespace Hangfire.Redis.Tests
{
    public class RedisFetchedJobFacts
    {
        private const string JobId = "id";
        private const string Queue = "queue";

        private readonly RedisStorage _storage;

        public RedisFetchedJobFacts()
        {

            var options = new RedisStorageOptions() { };
            _storage = new RedisStorage(RedisUtils.RedisClient, options);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>("storage",
                 () => new RedisFetchedJob(null, RedisUtils.RedisClient, JobId, Queue));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenRedisIsNull()
        {
            Assert.Throws<ArgumentNullException>("redisClient",
                () => new RedisFetchedJob(_storage, null, JobId, Queue));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenJobIdIsNull()
        {
            Assert.Throws<ArgumentNullException>("jobId",
                () => new RedisFetchedJob(_storage, RedisUtils.RedisClient, null, Queue));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenQueueIsNull()
        {
            Assert.Throws<ArgumentNullException>("queue",
                () => new RedisFetchedJob(_storage, RedisUtils.RedisClient, JobId, null));
        }

        [Fact, CleanRedis]
        public void RemoveFromQueue_RemovesJobFromTheFetchedList()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "job-id");

                var fetchedJob = new RedisFetchedJob(_storage, redis, "job-id", "my-queue");

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                Assert.Equal(0, redis.LLen("{hangfire}:queue:my-queue:dequeued"));
            });
        }

        [Fact, CleanRedis]
        public void RemoveFromQueue_RemovesOnlyJobWithTheSpecifiedId()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "job-id");
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "another-job-id");

                var fetchedJob = new RedisFetchedJob(_storage, redis, "job-id", "my-queue");

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                Assert.Equal(1, redis.LLen("{hangfire}:queue:my-queue:dequeued"));
                Assert.Equal("another-job-id", (string)redis.RPop("{hangfire}:queue:my-queue:dequeued"));
            });
        }

        [Fact, CleanRedis]
        public void RemoveFromQueue_RemovesOnlyOneJob()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "job-id");
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "job-id");

                var fetchedJob = new RedisFetchedJob(_storage, redis, "job-id", "my-queue");

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                Assert.Equal(1, redis.LLen("{hangfire}:queue:my-queue:dequeued"));
            });
        }

        [Fact, CleanRedis]
        public void RemoveFromQueue_RemovesTheFetchedFlag()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.HSet("{hangfire}:job:my-job", "Fetched", "value");
                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue");

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                Assert.False(redis.HExists("{hangfire}:job:my-job", "Fetched"));
            });
        }

        [Fact, CleanRedis]
        public void RemoveFromQueue_RemovesTheCheckedFlag()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.HSet("{hangfire}:job:my-job", "Checked", "value");
                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue");

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                Assert.False(redis.HExists("{hangfire}:job:my-job", "Checked"));
            });
        }

        [Fact, CleanRedis]
        public void Requeue_PushesAJobBackToQueue()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "my-job");
                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue");

                // Act
                fetchedJob.Requeue();

                // Assert
                Assert.Equal("my-job", (string)redis.RPop("{hangfire}:queue:my-queue"));
            });
        }

        [Fact, CleanRedis]
        public void Requeue_PushesAJobToTheRightSide()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.RPush("{hangfire}:queue:my-queue", "another-job");
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "my-job");

                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue");

                // Act
                fetchedJob.Requeue();

                // Assert - RPOP
                Assert.Equal("my-job", (string)redis.RPop("{hangfire}:queue:my-queue"));
            });
        }

        [Fact, CleanRedis]
        public void Requeue_RemovesAJobFromFetchedList()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "my-job");
                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue");

                // Act
                fetchedJob.Requeue();

                // Assert
                Assert.Equal(0, redis.LLen("{hangfire}:queue:my-queue:dequeued"));
            });
        }

        [Fact, CleanRedis]
        public void Requeue_RemovesTheFetchedFlag()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.HSet("{hangfire}:job:my-job", "Fetched", "value");
                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue");

                // Act
                fetchedJob.Requeue();

                // Assert
                Assert.False(redis.HExists("{hangfire}:job:my-job", "Fetched"));
            });
        }

        [Fact, CleanRedis]
        public void Requeue_RemovesTheCheckedFlag()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.HSet("{hangfire}:job:my-job", "Checked", "value");
                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue");

                // Act
                fetchedJob.Requeue();

                // Assert
                Assert.False(redis.HExists("{hangfire}:job:my-job", "Checked"));
            });
        }

        [Fact, CleanRedis]
        public void Dispose_WithNoComplete_RequeuesAJob()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "my-job");
                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue");

                // Act
                fetchedJob.Dispose();

                // Assert
                Assert.Equal(1, redis.LLen("{hangfire}:queue:my-queue"));
            });
        }

        [Fact, CleanRedis]
        public void Dispose_AfterRemoveFromQueue_DoesNotRequeueAJob()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "my-job");
                redis.RPush("{hangfire}:queue:my-queue:dequeued", "my-job");
                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue");

                // Act
                fetchedJob.RemoveFromQueue();
                fetchedJob.Dispose();

                // Assert
                Assert.Equal(0, redis.LLen("{hangfire}:queue:my-queue"));
            });
        }

        private static void UseRedis(Action<CSRedis.CSRedisClient> action)
        {
            var redis = RedisUtils.RedisClient;
            action(redis);
        }
    }
}
