using System;
using System.Collections.Generic;
using Hangfire.Common;
using Hangfire.States;
using Moq;
using Xunit;

namespace Hangfire.Redis.Tests
{
    public class RedisWriteOnlyTransactionFacts
    {
        private readonly RedisStorage _storage;

        public RedisWriteOnlyTransactionFacts()
        {
            var options = new RedisStorageOptions();
            _storage = new RedisStorage(RedisUtils.RedisClient, options);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>("storage",
                () => new RedisWriteOnlyTransaction(null));
        }

        //[Fact]
        //public void Ctor_ThrowsAnException_WhenTransactionIsNull()
        //{
        //    Assert.Throws<ArgumentNullException>("transaction",
        //        () => new RedisWriteOnlyTransaction(_storage, null));
        //}1

        [Fact, CleanRedis]
        public void ExpireJob_SetsExpirationDateForAllRelatedKeys()
        {
            UseConnection(redis =>
            {
                // Arrange
                redis.Set("{hangfire}:job:my-job", "job");
                redis.Set("{hangfire}:job:my-job:state", "state");
                redis.Set("{hangfire}:job:my-job:history", "history");

                // Act
                Commit(redis, x => x.ExpireJob("my-job", TimeSpan.FromDays(1)));

                // Assert
                var jobEntryTtl = redis.Ttl("{hangfire}:job:my-job");
                var stateEntryTtl = redis.Ttl("{hangfire}:job:my-job:state");
                var historyEntryTtl = redis.Ttl("{hangfire}:job:my-job:state");

                Assert.True(TimeSpan.FromHours(23) < TimeSpan.FromSeconds(jobEntryTtl) && TimeSpan.FromSeconds(jobEntryTtl) < TimeSpan.FromHours(25));
                Assert.True(TimeSpan.FromHours(23) < TimeSpan.FromSeconds(stateEntryTtl) && TimeSpan.FromSeconds(stateEntryTtl) < TimeSpan.FromHours(25));
                Assert.True(TimeSpan.FromHours(23) < TimeSpan.FromSeconds(historyEntryTtl) && TimeSpan.FromSeconds(historyEntryTtl) < TimeSpan.FromHours(25));
            });
        }

        [Fact, CleanRedis]
        public void SetJobState_ModifiesJobEntry()
        {
            UseConnection(redis =>
            {
                // Arrange
                var state = new Mock<IState>();
                state.Setup(x => x.SerializeData()).Returns(new Dictionary<string, string>());
                state.Setup(x => x.Name).Returns("my-state");

                // Act
                Commit(redis, x => x.SetJobState("my-job", state.Object));

                // Assert
                var hash = redis.HGetAll("{hangfire}:job:my-job");
                Assert.Equal("my-state", hash["State"]);
            });
        }

        [Fact, CleanRedis]
        public void SetJobState_RewritesStateEntry()
        {
            UseConnection(redis =>
            {
                // Arrange
                redis.HSet("{hangfire}:job:my-job:state", "OldName", "OldValue");

                var state = new Mock<IState>();
                state.Setup(x => x.SerializeData()).Returns(
                    new Dictionary<string, string>
                    {
                        { "Name", "Value" }
                    });
                state.Setup(x => x.Name).Returns("my-state");
                state.Setup(x => x.Reason).Returns("my-reason");

                // Act
                Commit(redis, x => x.SetJobState("my-job", state.Object));

                // Assert
                var stateHash = redis.HGetAll("{hangfire}:job:my-job:state");
                Assert.False(stateHash.ContainsKey("OldName"));
                Assert.Equal("my-state", stateHash["State"]);
                Assert.Equal("my-reason", stateHash["Reason"]);
                Assert.Equal("Value", stateHash["Name"]);
            });
        }

        [Fact, CleanRedis]
        public void SetJobState_AppendsJobHistoryList()
        {
            UseConnection(redis =>
            {
                // Arrange
                var state = new Mock<IState>();
                state.Setup(x => x.Name).Returns("my-state");
                state.Setup(x => x.SerializeData()).Returns(new Dictionary<string, string>());

                // Act
                Commit(redis, x => x.SetJobState("my-job", state.Object));

                // Assert
                Assert.Equal(1, redis.LLen("{hangfire}:job:my-job:history"));
            });
        }

        [Fact, CleanRedis]
        public void PersistJob_RemovesExpirationDatesForAllRelatedKeys()
        {
            UseConnection(redis =>
            {
                // Arrange
                redis.Set("{hangfire}:job:my-job", "job", TimeSpan.FromDays(1));
                redis.Set("{hangfire}:job:my-job:state", "state", TimeSpan.FromDays(1));
                redis.Set("{hangfire}:job:my-job:history", "history", TimeSpan.FromDays(1));

                // Act
                Commit(redis, x => x.PersistJob("my-job"));

                // Assert
                Assert.Equal(-1, redis.Ttl("{hangfire}:job:my-job"));
                Assert.Equal(-1, redis.Ttl("{hangfire}:job:my-job:state"));
                Assert.Equal(-1, redis.Ttl("{hangfire}:job:my-job:history"));
            });
        }

        [Fact, CleanRedis]
        public void AddJobState_AddsJobHistoryEntry_AsJsonObject()
        {
            UseConnection(redis =>
            {
                // Arrange
                var state = new Mock<IState>();
                state.Setup(x => x.Name).Returns("my-state");
                state.Setup(x => x.Reason).Returns("my-reason");
                state.Setup(x => x.SerializeData()).Returns(
                    new Dictionary<string, string> { { "Name", "Value" } });

                // Act
                Commit(redis, x => x.AddJobState("my-job", state.Object));

                // Assert
                var serializedEntry = redis.LIndex("{hangfire}:job:my-job:history", 0);
                Assert.False(string.IsNullOrWhiteSpace(serializedEntry));


                var entry = SerializationHelper.Deserialize<Dictionary<string, string>>(serializedEntry);
                Assert.Equal("my-state", entry["State"]);
                Assert.Equal("my-reason", entry["Reason"]);
                Assert.Equal("Value", entry["Name"]);
                Assert.True(entry.ContainsKey("CreatedAt"));
            });
        }

        [Fact, CleanRedis]
        public void AddToQueue_AddsSpecifiedJobToTheQueue()
        {
            UseConnection(redis =>
            {
                Commit(redis, x => x.AddToQueue("critical", "my-job"));

                Assert.True(redis.SIsMember("{hangfire}:queues", "critical"));
                Assert.Equal("my-job", (string)redis.LIndex("{hangfire}:queue:critical", 0));
            });
        }

        [Fact, CleanRedis]
        public void AddToQueue_PrependsListWithJob()
        {
            UseConnection(redis =>
            {
                redis.LPush("{hangfire}:queue:critical", "another-job");

                Commit(redis, x => x.AddToQueue("critical", "my-job"));

                Assert.Equal("my-job", (string)redis.LIndex("{hangfire}:queue:critical", 0));
            });
        }

        [Fact, CleanRedis]
        public void IncrementCounter_IncrementValueEntry()
        {
            UseConnection(redis =>
            {
                redis.Set("{hangfire}:entry", "3");

                Commit(redis, x => x.IncrementCounter("entry"));

                Assert.Equal("4", (string)redis.Get("{hangfire}:entry"));
                Assert.Equal(-1, redis.Ttl("{hangfire}:entry"));
            });
        }

        [Fact, CleanRedis]
        public void IncrementCounter_WithExpiry_IncrementsValueAndSetsExpirationDate()
        {
            UseConnection(redis =>
            {
                redis.Set("{hangfire}:entry", "3");

                Commit(redis, x => x.IncrementCounter("entry", TimeSpan.FromDays(1)));

                var entryTtl = redis.Ttl("{hangfire}:entry");
                Assert.Equal("4", (string)redis.Get("{hangfire}:entry"));
                Assert.True(TimeSpan.FromHours(23) < TimeSpan.FromSeconds(entryTtl) && TimeSpan.FromSeconds(entryTtl) < TimeSpan.FromHours(25));
            });
        }

        [Fact, CleanRedis]
        public void DecrementCounter_DecrementsTheValueEntry()
        {
            UseConnection(redis =>
            {
                redis.Set("{hangfire}:entry", "3");

                Commit(redis, x => x.DecrementCounter("entry"));

                Assert.Equal("2", (string)redis.Get("{hangfire}:entry"));
                Assert.Equal(-1, redis.Ttl("{hangfire}:entry"));
            });
        }

        [Fact, CleanRedis]
        public void DecrementCounter_WithExpiry_DecrementsTheValueAndSetsExpirationDate()
        {
            UseConnection(redis =>
            {
                redis.Set("{hangfire}:entry", "3");
                Commit(redis, x => x.DecrementCounter("entry", TimeSpan.FromDays(1)));
                var b = redis.Ttl("{hangfire}:entry");
                var entryTtl = redis.Ttl("{hangfire}:entry");
                Assert.Equal("2", (string)redis.Get("{hangfire}:entry"));
                Assert.True(TimeSpan.FromHours(23) < TimeSpan.FromSeconds(entryTtl) && TimeSpan.FromSeconds(entryTtl) < TimeSpan.FromHours(25));
            });
        }

        [Fact, CleanRedis]
        public void AddToSet_AddsItemToSortedSet()
        {
            UseConnection(redis =>
            {
                Commit(redis, x => x.AddToSet("my-set", "my-value"));

                Assert.True(redis.ZRank("{hangfire}:my-set", "my-value").HasValue);
            });
        }

        [Fact, CleanRedis]
        public void AddToSet_WithScore_AddsItemToSortedSetWithScore()
        {
            UseConnection(redis =>
            {
                Commit(redis, x => x.AddToSet("my-set", "my-value", 3.2));

                Assert.True(redis.ZRank("{hangfire}:my-set", "my-value").HasValue);
                Assert.Equal(3.2M, redis.ZScore("{hangfire}:my-set", "my-value").Value, 3);

            });
        }

        [Fact, CleanRedis]
        public void RemoveFromSet_RemoveSpecifiedItemFromSortedSet()
        {
            UseConnection(redis =>
            {
                redis.ZAdd("{hangfire}:my-set", (0, "my-value"));

                Commit(redis, x => x.RemoveFromSet("my-set", "my-value"));

                Assert.False(redis.ZRank("{hangfire}:my-set", "my-value").HasValue);
            });
        }

        [Fact, CleanRedis]
        public void InsertToList_PrependsListWithSpecifiedValue()
        {
            UseConnection(redis =>
            {
                redis.RPush("{hangfire}:list", "value");

                Commit(redis, x => x.InsertToList("list", "new-value"));
                Assert.Equal("new-value", (string)redis.LIndex("{hangfire}:list", 0));
            });
        }

        [Fact, CleanRedis]
        public void RemoveFromList_RemovesAllGivenValuesFromList()
        {
            UseConnection(redis =>
            {
                redis.RPush("{hangfire}:list", "value");
                redis.RPush("{hangfire}:list", "another-value");
                redis.RPush("{hangfire}:list", "value");

                Commit(redis, x => x.RemoveFromList("list", "value"));

                Assert.Equal(1, redis.LLen("{hangfire}:list"));
                Assert.Equal("another-value", (string)redis.LIndex("{hangfire}:list", 0));
            });
        }

        [Fact, CleanRedis]
        public void TrimList_TrimsListToASpecifiedRange()
        {
            UseConnection(redis =>
            {
                redis.RPush("{hangfire}:list", "1");
                redis.RPush("{hangfire}:list", "2");
                redis.RPush("{hangfire}:list", "3");
                redis.RPush("{hangfire}:list", "4");

                Commit(redis, x => x.TrimList("list", 1, 2));

                Assert.Equal(2, redis.LLen("{hangfire}:list"));
                Assert.Equal("2", (string)redis.LIndex("{hangfire}:list", 0));
                Assert.Equal("3", (string)redis.LIndex("{hangfire}:list", 1));
            });
        }

        [Fact, CleanRedis]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(redis =>
            {
                Assert.Throws<ArgumentNullException>("key",
                    () => Commit(redis, x => x.SetRangeInHash(null, new Dictionary<string, string>())));
            });
        }

        [Fact, CleanRedis]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            UseConnection(redis =>
            {
                Assert.Throws<ArgumentNullException>("keyValuePairs",
                    () => Commit(redis, x => x.SetRangeInHash("some-hash", null)));
            });
        }

        [Fact, CleanRedis]
        public void SetRangeInHash_SetsAllGivenKeyPairs()
        {
            UseConnection(redis =>
            {
                Commit(redis, x => x.SetRangeInHash("some-hash", new Dictionary<string, string>
                {
                    { "Key1", "Value1" },
                    { "Key2", "Value2" }
                }));

                var hash = redis.HGetAll("{hangfire}:some-hash");
                Assert.Equal("Value1", hash["Key1"]);
                Assert.Equal("Value2", hash["Key2"]);
            });
        }

        [Fact, CleanRedis]
        public void RemoveHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(redis =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => Commit(redis, x => x.RemoveHash(null)));
            });
        }

        [Fact, CleanRedis]
        public void RemoveHash_RemovesTheCorrespondingEntry()
        {
            UseConnection(redis =>
            {
                redis.HSet("{hangfire}:some-hash", "key", "value");

                Commit(redis, x => x.RemoveHash("some-hash"));

                var hash = redis.HGetAll("{hangfire}:some-hash");
                Assert.Empty(hash);
            });
        }

        private void Commit(CSRedis.CSRedisClient redis, Action<RedisWriteOnlyTransaction> action)
        {
            using (var transaction = new RedisWriteOnlyTransaction(_storage))
            {
                action(transaction);
                transaction.Commit();
            }
        }

        private static void UseConnection(Action<CSRedis.CSRedisClient> action)
        {
            var redis = RedisUtils.RedisClient;
            action(redis);
        }
    }
}
