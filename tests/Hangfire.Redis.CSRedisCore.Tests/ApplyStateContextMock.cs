using System;
using Hangfire.Common;
using Hangfire.States;
using Moq;

namespace Hangfire.Redis.Tests
{
    public class ApplyStateContextMock
    {
        private readonly Lazy<ApplyStateContext> _context;

        public ApplyStateContextMock(string jobId)
        {
            NewStateValue = new Mock<IState>().Object;
            OldStateValue = null;
            var storage = CreateStorage();
            var connection = storage.GetConnection();
            var writeOnlyTransaction = connection.CreateWriteTransaction();
            var job = new Job(this.GetType().GetMethod("GetType"));
            var backgroundJob = new BackgroundJob(jobId, job, DateTime.MinValue);
            _context = new Lazy<ApplyStateContext>(() => new ApplyStateContext(storage, connection,
                writeOnlyTransaction, backgroundJob, NewStateValue, OldStateValue));
        }

        public IState NewStateValue { get; set; }

        public string OldStateValue { get; set; }

        public ApplyStateContext Object => _context.Value;

        private RedisStorage CreateStorage()
        {
            var options = new RedisStorageOptions();
            return new RedisStorage(RedisUtils.RedisClient, options);
        }
    }
}
