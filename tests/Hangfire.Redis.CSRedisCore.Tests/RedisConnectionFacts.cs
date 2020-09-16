using System;
using System.Collections.Generic;
using Xunit;

namespace Hangfire.Redis.Tests
{
    public class RedisConnectionFacts
    {
        private readonly RedisStorage _storage;

        public RedisConnectionFacts()
        {
            var options = new RedisStorageOptions() { };
            _storage = new RedisStorage(RedisUtils.RedisClient, options);
        }

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

        private void UseConnections(Action<CSRedis.CSRedisClient, RedisConnection> action)
        {
            var subscription = new RedisSubscription(_storage, RedisUtils.RedisClient);

            using (var connection = new RedisConnection(_storage, RedisUtils.RedisClient, subscription, new RedisStorageOptions().FetchTimeout))
            {
                action(RedisUtils.RedisClient, connection);
            }
        }

        private void UseConnection(Action<RedisConnection> action)
        {
            var subscription = new RedisSubscription(_storage, RedisUtils.RedisClient);

            using (var connection = new RedisConnection(_storage, RedisUtils.RedisClient, subscription, new RedisStorageOptions().FetchTimeout))
            {
                action(connection);
            }
        }
    }
}
