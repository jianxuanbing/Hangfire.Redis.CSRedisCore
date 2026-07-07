using System;
using System.Collections.Generic;
using System.Threading;
using Hangfire.Common;
using Hangfire.Storage;
using Xunit;

namespace Hangfire.Redis.Tests
{
    public class RedisConnectionFacts : IDisposable
    {
        private readonly RedisStorage _storage;

        public RedisConnectionFacts()
        {
            var options = new RedisStorageOptions() { };
            _storage = new RedisStorage(RedisUtils.RedisClient, options);
        }

        public void Dispose() => _storage.Dispose();

        [Fact, CleanRedis]
        public void GetStateData_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(
                connection => Assert.Throws<ArgumentNullException>("jobId",
                    () => connection.GetStateData(null)));
        }

        [Fact, CleanRedis]
        public void GetStateData_ReturnsNull_WhenJobDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetStateData("random-id");
                Assert.Null(result);
            });
        }

        [Fact, CleanRedis]
        public void GetStateData_ReturnsCorrectResult()
        {
            UseConnections((redis, connection) =>
            {
                redis.HMSet(
                    "{hangfire}:job:my-job:state",
                    new Dictionary<string, string>
                    {
                        { "State", "Name" },
                        { "Reason", "Reason" },
                        { "Key", "Value" }
                    }.DicToObjectArray());

                var result = connection.GetStateData("my-job");

                Assert.NotNull(result);
                Assert.Equal("Name", result.Name);
                Assert.Equal("Reason", result.Reason);
                Assert.Equal("Value", result.Data["Key"]);
            });
        }

        [Fact, CleanRedis]
        public void GetStateData_ReturnsNullReason_IfThereIsNoSuchKey()
        {
            UseConnections((redis, connection) =>
            {
                redis.HMSet(
                    "{hangfire}:job:my-job:state",
                    new Dictionary<string, string>
                    {
                        { "State", "Name" }
                    }.DicToObjectArray());

                var result = connection.GetStateData("my-job");

                Assert.NotNull(result);
                Assert.Null(result.Reason);
            });
        }

        [Fact, CleanRedis]
        public void GetAllItemsFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
                Assert.Throws<ArgumentNullException>("key",
                    () => connection.GetAllItemsFromSet(null)));
        }

        [Fact, CleanRedis]
        public void GetAllItemsFromSet_ReturnsEmptyCollection_WhenSetDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetAllItemsFromSet("some-set");

                Assert.NotNull(result);
                Assert.Empty(result);
            });
        }

        [Fact, CleanRedis]
        public void GetAllItemsFromSet_ReturnsAllItems()
        {
            UseConnections((redis, connection) =>
            {
                // Arrange
                redis.ZAdd("{hangfire}:some-set", (0, "1"));
                redis.ZAdd("{hangfire}:some-set", (0, "2"));

                // Act
                var result = connection.GetAllItemsFromSet("some-set");

                // Assert
                Assert.Equal(2, result.Count);
                Assert.Contains("1", result);
                Assert.Contains("2", result);
            });
        }

        [Fact, CleanRedis]
        public void GetUtcDateTime_ReturnsReasonableUtcTime()
        {
            UseConnection(connection =>
            {
                var utcNow = DateTime.UtcNow;
                var redisUtcNow = connection.GetUtcDateTime();

                Assert.Equal(DateTimeKind.Utc, redisUtcNow.Kind);
                Assert.InRange(redisUtcNow, utcNow.AddSeconds(-5), DateTime.UtcNow.AddSeconds(5));
            });
        }

        [Fact, CleanRedis]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>("key",
                    () => connection.SetRangeInHash(null, new Dictionary<string, string>()));
            });
        }

        [Fact, CleanRedis]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>("keyValuePairs",
                    () => connection.SetRangeInHash("some-hash", null));
            });
        }

        [Fact, CleanRedis]
        public void SetRangeInHash_SetsAllGivenKeyPairs()
        {
            UseConnections((redis, connection) =>
            {
                connection.SetRangeInHash("some-hash", new Dictionary<string, string>
                {
                    { "Key1", "Value1" },
                    { "Key2", "Value2" }
                });

                var hash = redis.HGetAll("{hangfire}:some-hash");
                Assert.Equal("Value1", hash["Key1"]);
                Assert.Equal("Value2", hash["Key2"]);
            });
        }

        [Fact, CleanRedis]
        public void GetAllEntriesFromHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
                Assert.Throws<ArgumentNullException>(() => connection.GetAllEntriesFromHash(null)));
        }

        [Fact, CleanRedis]
        public void GetAllEntriesFromHash_ReturnsNullValue_WhenHashDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetAllEntriesFromHash("some-hash");
                Assert.Null(result);
            });
        }

        [Fact, CleanRedis]
        public void GetAllEntriesFromHash_ReturnsAllEntries()
        {
            UseConnections((redis, connection) =>
            {
                // Arrange
                redis.HMSet("{hangfire}:some-hash", new Dictionary<string, string>
                {
                    { "Key1", "Value1" },
                    { "Key2", "Value2" }
                }.DicToObjectArray());

                // Act
                var result = connection.GetAllEntriesFromHash("some-hash");

                // Assert
                Assert.NotNull(result);
                Assert.Equal("Value1", result["Key1"]);
                Assert.Equal("Value2", result["Key2"]);
            });
        }

        [Fact, CleanRedis]
        public void CreateExpiredJob_AndGetJobData_ReturnsStoredJobData()
        {
            UseConnection(connection =>
            {
                var createdAt = new DateTime(2026, 7, 7, 12, 0, 0, DateTimeKind.Utc);
                var job = Job.FromExpression(() => SampleMethods.NoArgs());
                var jobId = connection.CreateExpiredJob(
                    job,
                    new Dictionary<string, string> { { "TraceId", "trace-1" } },
                    createdAt,
                    TimeSpan.FromHours(1));

                var jobData = connection.GetJobData(jobId);

                Assert.NotNull(jobData);
                Assert.NotNull(jobData.Job);
                Assert.NotNull(jobData.InvocationData);
                Assert.Equal(job.Type, jobData.Job.Type);
                Assert.Equal(job.Method.Name, jobData.Job.Method.Name);
                Assert.Equal(createdAt, jobData.CreatedAt);
                Assert.Equal("trace-1", jobData.ParametersSnapshot["TraceId"]);
                Assert.Null(jobData.Job.Queue);
                Assert.Null(jobData.InvocationData.Queue);
            });
        }

        [Fact, CleanRedis]
        public void CreateExpiredJob_AndGetJobData_ReturnsStoredQueue()
        {
            UseConnection(connection =>
            {
                var job = Job.FromExpression(() => SampleMethods.NoArgs(), "critical");
                var jobId = connection.CreateExpiredJob(
                    job,
                    new Dictionary<string, string>(),
                    DateTime.UtcNow,
                    TimeSpan.FromHours(1));

                var jobData = connection.GetJobData(jobId);

                Assert.NotNull(jobData);
                Assert.Equal("critical", jobData.Job.Queue);
                Assert.Equal("critical", jobData.InvocationData.Queue);
            });
        }

        [Fact, CleanRedis]
        public void GetJobData_ReturnsJob_WhenLegacyHashDoesNotContainQueue()
        {
            UseConnections((redis, connection) =>
            {
                var job = Job.FromExpression(() => SampleMethods.NoArgs());
                var invocationData = InvocationData.SerializeJob(job);
                redis.HMSet("{hangfire}:job:legacy-job", new Dictionary<string, string>
                {
                    { "Type", invocationData.Type },
                    { "Method", invocationData.Method },
                    { "ParameterTypes", invocationData.ParameterTypes },
                    { "Arguments", invocationData.Arguments },
                    { "CreatedAt", JobHelper.SerializeDateTime(DateTime.UtcNow) }
                }.DicToObjectArray());

                var jobData = connection.GetJobData("legacy-job");

                Assert.NotNull(jobData);
                Assert.NotNull(jobData.Job);
                Assert.Null(jobData.Job.Queue);
                Assert.Null(jobData.InvocationData.Queue);
            });
        }

        [Fact, CleanRedis]
        public void FetchNextJob_ConsumesDefaultQueueJob()
        {
            UseConnection(connection =>
            {
                var jobId = connection.CreateExpiredJob(
                    Job.FromExpression(() => SampleMethods.NoArgs()),
                    new Dictionary<string, string>(),
                    DateTime.UtcNow,
                    TimeSpan.FromHours(1));

                using (var transaction = new RedisWriteOnlyTransaction(_storage))
                {
                    transaction.AddToQueue("default", jobId);
                    transaction.Commit();
                }

                using (var fetchedJob = connection.FetchNextJob(new[] { "default" }, CancellationToken.None))
                {
                    Assert.Equal(jobId, fetchedJob.JobId);
                    fetchedJob.RemoveFromQueue();
                }
            });
        }

        [Fact, CleanRedis]
        public void FetchNextJob_ConsumesCustomQueueJob()
        {
            UseConnection(connection =>
            {
                var job = Job.FromExpression(() => SampleMethods.NoArgs(), "critical");
                var jobId = connection.CreateExpiredJob(
                    job,
                    new Dictionary<string, string>(),
                    DateTime.UtcNow,
                    TimeSpan.FromHours(1));

                using (var transaction = new RedisWriteOnlyTransaction(_storage))
                {
                    transaction.AddToQueue("critical", jobId);
                    transaction.Commit();
                }

                using (var fetchedJob = connection.FetchNextJob(new[] { "critical" }, CancellationToken.None))
                {
                    Assert.Equal(jobId, fetchedJob.JobId);
                    Assert.Equal("critical", connection.GetJobData(jobId).Job.Queue);
                    fetchedJob.RemoveFromQueue();
                }
            });
        }

        public static class SampleMethods
        {
            public static void NoArgs()
            {
            }
        }

        private void UseConnections(Action<CSRedis.CSRedisClient, RedisConnection> action)
        {
            using (var connection = (RedisConnection)_storage.GetConnection())
            {
                action(RedisUtils.RedisClient, connection);
            }
        }

        private void UseConnection(Action<RedisConnection> action)
        {
            using (var connection = (RedisConnection)_storage.GetConnection())
            {
                action(connection);
            }
        }
    }
}
