using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Hangfire;
using Hangfire.Common;
using Hangfire.Dashboard;
using System.Linq;
using Hangfire.Redis.States;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.AspNetCore.Builder;
using Xunit;
using Xunit.Sdk;

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
            Assert.True(storage.HasFeature(JobStorageFeatures.Connection.BatchedGetFirstByLowest));
            Assert.True(storage.HasFeature(JobStorageFeatures.Connection.GetUtcDateTime));
            Assert.True(storage.HasFeature(JobStorageFeatures.JobQueueProperty));
            Assert.True(storage.HasFeature(JobStorageFeatures.Transaction.CreateJob));
            Assert.True(storage.HasFeature(JobStorageFeatures.Transaction.SetJobParameter));
            Assert.True(storage.HasFeature(JobStorageFeatures.Transaction.RemoveFromQueue(typeof(RedisFetchedJob))));
            Assert.False(storage.HasFeature(JobStorageFeatures.Transaction.AcquireDistributedLock));
            Assert.False(storage.HasFeature(JobStorageFeatures.Monitoring.DeletedStateGraphs));
            Assert.False(storage.HasFeature(JobStorageFeatures.Monitoring.AwaitingJobs));
        }

        [Fact, CleanRedis]
        public async Task BackgroundJobServer_ProcessesCriticalAndDefaultJobs()
        {
            using var storage = CreateStorage();
            GlobalConfiguration.Configuration.UseNoOpLogProvider();

            var client = new BackgroundJobClient(storage);

            var criticalJobId = client.Create(
                Hangfire.Common.Job.FromExpression(() => RedisUtils.RecordExecution("critical"), "critical"),
                new EnqueuedState());

            var defaultJobId = client.Create(
                Hangfire.Common.Job.FromExpression(() => RedisUtils.RecordExecution("default")),
                new EnqueuedState());

            using var server = new BackgroundJobServer(new BackgroundJobServerOptions
            {
                WorkerCount = 1,
                Queues = new[] { "critical", "default" },
                Activator = new JobActivator(),
                FilterProvider = JobFilterProviders.Providers
            }, storage);

            try
            {
                await WaitUntilAsync(() => RedisUtils.RedisClient.LLen(RedisUtils.ExecutedJobsKey) == 2, TimeSpan.FromSeconds(30));
            }
            catch (OperationCanceledException)
            {
                using var diagnosticConnection = storage.GetConnection();
                var criticalJob = diagnosticConnection.GetJobData(criticalJobId);
                var defaultJob = diagnosticConnection.GetJobData(defaultJobId);

                throw new XunitException(
                    $"Timed out waiting for jobs. " +
                    $"criticalState={criticalJob?.State ?? "<null>"}, " +
                    $"defaultState={defaultJob?.State ?? "<null>"}, " +
                    $"criticalQueue={RedisUtils.RedisClient.LLen("{hangfire}:queue:critical")}, " +
                    $"defaultQueue={RedisUtils.RedisClient.LLen("{hangfire}:queue:default")}, " +
                    $"criticalDequeued={RedisUtils.RedisClient.LLen("{hangfire}:queue:critical:dequeued")}, " +
                    $"defaultDequeued={RedisUtils.RedisClient.LLen("{hangfire}:queue:default:dequeued")}, " +
                    $"executed={string.Join(",", RedisUtils.RedisClient.LRange(RedisUtils.ExecutedJobsKey, 0, -1))}");
            }

            using var connection = storage.GetConnection();
            Assert.Equal(SucceededState.StateName, connection.GetJobData(criticalJobId).State);
            Assert.Equal(SucceededState.StateName, connection.GetJobData(defaultJobId).State);

            Assert.Equal(new[] { "critical", "default" }, RedisUtils.RedisClient.LRange(RedisUtils.ExecutedJobsKey, 0, -1).ToArray());
            Assert.Equal(0, RedisUtils.RedisClient.LLen("{hangfire}:queue:critical"));
            Assert.Equal(0, RedisUtils.RedisClient.LLen("{hangfire}:queue:default"));
            Assert.Equal(0, RedisUtils.RedisClient.LLen("{hangfire}:queue:critical:dequeued"));
            Assert.Equal(0, RedisUtils.RedisClient.LLen("{hangfire}:queue:default:dequeued"));
        }

        [Fact, CleanRedis]
        public async Task Dashboard_HomePage_ReturnsSuccess()
        {
            using var storage = CreateStorage();
            SeedDashboardData(storage);

            await AssertDashboardRoutesReturnSuccess(storage, "/hangfire/");
        }

        [Fact, CleanRedis]
        public async Task Dashboard_CommonPages_ReturnSuccess()
        {
            using var storage = CreateStorage();
            SeedDashboardData(storage);

            await AssertDashboardRoutesReturnSuccess(
                storage,
                "/hangfire/",
                "/hangfire/servers",
                "/hangfire/jobs/enqueued",
                "/hangfire/jobs/enqueued/default",
                "/hangfire/jobs/processing",
                "/hangfire/jobs/succeeded",
                "/hangfire/jobs/failed",
                "/hangfire/recurring");
        }

        [Fact, CleanRedis]
        public async Task Dashboard_JobDetails_ReturnsSuccess()
        {
            using var storage = CreateStorage();
            var jobId = CreateDashboardJob(storage);

            await AssertDashboardRoutesReturnSuccess(storage, $"/hangfire/jobs/details/{jobId}");
        }

        [Fact, CleanRedis]
        public async Task Dashboard_Stats_ReturnsSuccess()
        {
            using var storage = CreateStorage();
            SeedDashboardData(storage);

            await AssertDashboardStatsRouteReturnsSuccess(storage);
        }

        private static void SeedDashboardData(RedisStorage storage)
        {
            using var connection = storage.GetConnection();
            connection.AnnounceServer("server-1", new ServerContext
            {
                Queues = new[] { "default" },
                WorkerCount = 1
            });

            CreateDashboardJob(storage);
        }

        private static string CreateDashboardJob(RedisStorage storage)
        {
            using var connection = storage.GetConnection();

            var jobId = connection.CreateExpiredJob(
                Job.FromExpression(() => RedisUtils.RecordExecution("dashboard")),
                new System.Collections.Generic.Dictionary<string, string>(),
                DateTime.UtcNow,
                TimeSpan.FromHours(1));

            using (var transaction = connection.CreateWriteTransaction())
            {
                transaction.AddToQueue("default", jobId);
                transaction.Commit();
            }

            return jobId;
        }

        private static async Task AssertDashboardRoutesReturnSuccess(RedisStorage storage, params string[] routes)
        {
            JobStorage.Current = storage;

            try
            {
                using var server = new TestServer(new WebHostBuilder()
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
                        app.Run(_ => Task.CompletedTask);
                    }));

                var client = server.CreateClient();

                foreach (var route in routes)
                {
                    var response = await client.GetAsync(route);
                    Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                }
            }
            finally
            {
                JobStorage.Current = null;
            }
        }

        private static async Task AssertDashboardStatsRouteReturnsSuccess(RedisStorage storage)
        {
            JobStorage.Current = storage;

            try
            {
                using var server = new TestServer(new WebHostBuilder()
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
                        app.Run(_ => Task.CompletedTask);
                    }));

                using var request = new HttpRequestMessage(HttpMethod.Post, "/hangfire/stats")
                {
                    Content = new FormUrlEncodedContent(Array.Empty<KeyValuePair<string, string>>())
                };

                var response = await server.CreateClient().SendAsync(request);
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            }
            finally
            {
                JobStorage.Current = null;
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

        private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
        {
            using var cts = new CancellationTokenSource(timeout);

            while (!condition())
            {
                cts.Token.ThrowIfCancellationRequested();
                await Task.Delay(100, cts.Token);
            }
        }
    }
}
