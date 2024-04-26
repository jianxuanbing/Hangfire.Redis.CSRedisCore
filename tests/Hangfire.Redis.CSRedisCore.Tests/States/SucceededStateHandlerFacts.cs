﻿using Hangfire.Redis.States;
using Hangfire.States;
using Hangfire.Storage;
using Moq;
using Xunit;

namespace Hangfire.Redis.Tests.States
{
    [CleanRedis, Collection("Sequential")]
    public class SucceededStateHandlerFacts
    {
        private const string JobId = "1";

        private readonly ApplyStateContextMock _context;
        private readonly Mock<IWriteOnlyTransaction> _transaction;

        public SucceededStateHandlerFacts()
        {
            _context = new ApplyStateContextMock(JobId);
            _transaction = new Mock<IWriteOnlyTransaction>();
        }

        [Fact]
        public void StateName_ShouldBeEqualToSucceededState()
        {
            var handler = new SucceededStateHandler();
            Assert.Equal(SucceededState.StateName, (string) handler.StateName);
        }

        [Fact]
        public void Apply_ShouldInsertTheJob_ToTheBeginningOfTheSucceededList_AndTrimIt()
        {
            var handler = new SucceededStateHandler();
            handler.Apply(_context.Object, _transaction.Object);

            _transaction.Verify(x => x.InsertToList(
                "succeeded", JobId));
            _transaction.Verify(x => x.TrimList(
                "succeeded", 0, 499));
        }

        [Fact]
        public void Unapply_ShouldRemoveTheJob_FromTheSucceededList()
        {
            var handler = new SucceededStateHandler();
            handler.Unapply(_context.Object, _transaction.Object);

            _transaction.Verify(x => x.RemoveFromList("succeeded", JobId));
        }
    }
}
