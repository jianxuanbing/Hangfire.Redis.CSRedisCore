using System.Reflection;
using System.Threading;
using Xunit.Sdk;

// ReSharper disable once CheckNamespace
namespace Hangfire.Redis.Tests
{
    public class CleanRedisAttribute : BeforeAfterTestAttribute
    {
        private static readonly SemaphoreSlim GlobalLock = new SemaphoreSlim(1, 1);
        private bool _lockAcquired;

        public override void Before(MethodInfo methodUnderTest)
        {
            GlobalLock.Wait();
            _lockAcquired = true;
            var client = RedisUtils.RedisClient;
            client.NodesServerManager.FlushDb();
        }

        public override void After(MethodInfo methodUnderTest)
        {
            if (_lockAcquired)
            {
                _lockAcquired = false;
                GlobalLock.Release();
            }
        }
    }
}
