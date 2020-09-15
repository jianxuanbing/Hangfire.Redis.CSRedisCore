using System;
using System.Collections.Generic;
using CSRedis;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Redis
{
    /// <summary>
    /// Redis只写事务
    /// </summary>
    internal class RedisWriteOnlyTransaction : JobStorageTransaction
    {
        /// <summary>
        /// Redis存储
        /// </summary>
        private readonly RedisStorage _storage;

        /// <summary>
        /// Redis客户端管道
        /// </summary>
        private readonly CSRedisClientPipe<string> _redisClientPipe;

        /// <summary>
        /// 初始化一个<see cref="RedisWriteOnlyTransaction"/>类型的实例
        /// </summary>
        /// <param name="storage">Redis存储</param>
        public RedisWriteOnlyTransaction(RedisStorage storage)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _redisClientPipe = _storage.RedisClient.StartPipe();
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            throw new NotImplementedException();
        }

        public override void PersistJob(string jobId)
        {
            throw new NotImplementedException();
        }

        public override void SetJobState(string jobId, IState state)
        {
            throw new NotImplementedException();
        }

        public override void AddJobState(string jobId, IState state)
        {
            throw new NotImplementedException();
        }

        public override void AddToQueue(string queue, string jobId)
        {
            throw new NotImplementedException();
        }

        public override void IncrementCounter(string key)
        {
            throw new NotImplementedException();
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            throw new NotImplementedException();
        }

        public override void DecrementCounter(string key)
        {
            throw new NotImplementedException();
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            throw new NotImplementedException();
        }

        public override void AddToSet(string key, string value)
        {
            throw new NotImplementedException();
        }

        public override void AddToSet(string key, string value, double score)
        {
            throw new NotImplementedException();
        }

        public override void RemoveFromSet(string key, string value)
        {
            throw new NotImplementedException();
        }

        public override void InsertToList(string key, string value)
        {
            throw new NotImplementedException();
        }

        public override void RemoveFromList(string key, string value)
        {
            throw new NotImplementedException();
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            throw new NotImplementedException();
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            throw new NotImplementedException();
        }

        public override void RemoveHash(string key)
        {
        }

        public override void Commit()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// 释放资源
        /// </summary>
        public override void Dispose() => _redisClientPipe.Dispose();
    }
}
