using System;
using System.Net;
using System.Threading.Tasks;
using Hangfire;
using Hangfire.Dashboard;
using System.Linq;
using Hangfire.Redis.States;
using Hangfire.Storage;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Hangfire.Redis.Tests
{
    public class RedisStorageFacts
    {
        [Fact]
        public void Ctor_ThrowsAnException_WhenRedisClientIsNull()
        {
            Assert.Throws<ArgumentNullException>("redisClient", () => new RedisStorage((CSRedis.CSRedisClient)null));
        }

        [Fact, CleanRedis]
        public void GetStateHandlers_ReturnsAllHandlers()
        {
            using var storage = CreateStorage();

            var handlers = storage.GetStateHandlers();

            var handlerTypes = handlers.Select(x => x.GetType()).ToArray();
            Assert.Contains(typeof(FailedStateHandler), handlerTypes);
            Assert.Contains(typeof(ProcessingStateHandler), handlerTypes);
            Assert.Contains(typeof(SucceededStateHandler), handlerTypes);
            Assert.Contains(typeof(DeletedStateHandler), handlerTypes);
        }

        [Fact]
        public void HasFeature_ReturnsExpectedValues()
        {
            using var storage = CreateStorage();

            Assert.True(storage.HasFeature(JobStorageFeatures.ExtendedApi));
            Assert.True(storage.HasFeature(JobStorageFeatures.Connection.GetUtcDateTime));
            Assert.True(storage.HasFeature(JobStorageFeatures.JobQueueProperty));
            Assert.False(storage.HasFeature(JobStorageFeatures.Transaction.CreateJob));
            Assert.False(storage.HasFeature(JobStorageFeatures.Transaction.SetJobParameter));
            Assert.False(storage.HasFeature(JobStorageFeatures.Transaction.RemoveFromQueue(typeof(RedisFetchedJob))));
            Assert.False(storage.HasFeature(JobStorageFeatures.Transaction.AcquireDistributedLock));
            Assert.False(storage.HasFeature(JobStorageFeatures.Monitoring.DeletedStateGraphs));
            Assert.False(storage.HasFeature(JobStorageFeatures.Monitoring.AwaitingJobs));
        }

        [Fact, CleanRedis]
        public async Task Dashboard_HomePage_ReturnsSuccess()
        {
            using var storage = CreateStorage();

            using (var server = new TestServer(new WebHostBuilder()
                .ConfigureServices(services =>
                {
                    JobStorage.Current = storage;
                    services.AddHangfire(configuration => configuration.UseStorage(storage));
                })
                .Configure(app =>
                {
                    app.UseHangfireDashboard("/hangfire", new DashboardOptions
                    {
                        Authorization = new[] { new AllowAllDashboardAuthorizationFilter() },
                        IgnoreAntiforgeryToken = true
                    });
                })))
            {
                var response = await server.CreateClient().GetAsync("/hangfire/");
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            }
        }

        [Fact]
        public void Dispose_ThenGetConnection_ThrowsObjectDisposedException()
        {
            var storage = CreateStorage();
            storage.Dispose();

            Assert.Throws<ObjectDisposedException>(() => storage.GetConnection());
        }

        [Fact]
        public void Dispose_ThenGetMonitoringApi_ThrowsObjectDisposedException()
        {
            var storage = CreateStorage();
            storage.Dispose();

            Assert.Throws<ObjectDisposedException>(() => storage.GetMonitoringApi());
        }

        private RedisStorage CreateStorage()
        {
            var options = new RedisStorageOptions() { };
            return new RedisStorage(RedisUtils.RedisClient, options);
        }

        private sealed class AllowAllDashboardAuthorizationFilter : IDashboardAuthorizationFilter
        {
            public bool Authorize(DashboardContext context) => true;
        }
    }
}
