using System;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Moq;

namespace Hangfire.Redis.Tests
{
    public class ApplyStateContextMock : IDisposable
    {
        private readonly Lazy<ApplyStateContext> _context;
        private readonly IStorageConnection _connection;
        private readonly RedisStorage _storage;
        private readonly IWriteOnlyTransaction _writeOnlyTransaction;

        public ApplyStateContextMock(string jobId)
        {
            NewStateValue = new Mock<IState>().Object;
            OldStateValue = null;
            _storage = CreateStorage();
            _connection = _storage.GetConnection();
            _writeOnlyTransaction = _connection.CreateWriteTransaction();
            var job = new Job(this.GetType().GetMethod("GetType"));
            var backgroundJob = new BackgroundJob(jobId, job, DateTime.MinValue);
            _context = new Lazy<ApplyStateContext>(() => new ApplyStateContext(_storage, _connection,
                _writeOnlyTransaction, backgroundJob, NewStateValue, OldStateValue));
        }

        public IState NewStateValue { get; set; }

        public string OldStateValue { get; set; }

        public ApplyStateContext Object => _context.Value;

        public void Dispose()
        {
            _writeOnlyTransaction.Dispose();
            _connection.Dispose();
            _storage.Dispose();
        }

        private RedisStorage CreateStorage()
        {
            var options = new RedisStorageOptions();
            return new RedisStorage(RedisUtils.RedisClient, options);
        }
    }
}
