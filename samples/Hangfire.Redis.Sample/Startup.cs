using System;
using System.Collections.Generic;
using System.Globalization;
using Hangfire;
using Hangfire.Common;
using Hangfire.States;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Localization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Hangfire.Redis.Sample
{
    public class Startup
    {
        private readonly IConfiguration _configuration;

        public Startup(IConfiguration configuration)
        {
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        public void ConfigureServices(IServiceCollection services)
        {
            var storage = new RedisStorage(_configuration["Redis:ConnectionString"], new RedisStorageOptions
            {
                Prefix = _configuration["Redis:Prefix"] ?? RedisStorageOptions.DefaultPrefix
            });

            services.AddHangfire(o =>
            {
                o.SetDataCompatibilityLevel(CompatibilityLevel.Version_180);
                o.UseStorage(storage);
            });

            services.AddHangfireServer(options =>
            {
                options.Queues = _configuration.GetSection("Hangfire:Queues").Get<string[]>() ?? new[] { "critical", "default" };
            });

            JobStorage.Current = storage;
        }

        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }
            // 默认区域性
            var supportedCultures = new[]
            {
                new CultureInfo("zh-CN")
            };
            app.UseRequestLocalization(new RequestLocalizationOptions
            {
                DefaultRequestCulture = new RequestCulture("zh-CN"),
                // Formatting numbers, dates, etc.
                SupportedCultures = supportedCultures,
                // UI strings that we have localized.
                SupportedUICultures = supportedCultures,
                RequestCultureProviders = new List<IRequestCultureProvider>
                {
                    new QueryStringRequestCultureProvider(),
                    new CookieRequestCultureProvider(),
                    new AcceptLanguageHeaderRequestCultureProvider()
                }
            });

            app.UseHangfireDashboard("/hangfire", new DashboardOptions
            {
                IgnoreAntiforgeryToken = true,
                DisplayStorageConnectionString = false,
                IsReadOnlyFunc = context => false,
            });

            var client = new BackgroundJobClient();
            client.Create(
                Job.FromExpression(() => SampleJobs.Write("critical startup job"), "critical"),
                new EnqueuedState());

            client.Create(
                Job.FromExpression(() => SampleJobs.Write("default delayed job")),
                new ScheduledState(TimeSpan.FromMinutes(1)));

            RecurringJob.AddOrUpdate(
                "sample-default-recurring",
                () => SampleJobs.Write("default recurring job"),
                Cron.Minutely);

            app.Run(context =>
            {
                context.Response.Redirect("/hangfire");
                return System.Threading.Tasks.Task.CompletedTask;
            });
        }

        public static class SampleJobs
        {
            public static void Write(string message)
            {
                Console.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] {message}");
            }
        }
    }
}
