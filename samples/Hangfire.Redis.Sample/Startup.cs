using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Localization;
using Microsoft.Extensions.DependencyInjection;

namespace Hangfire.Redis.Sample
{
    public class Startup
    {
        // This method gets called by the runtime. Use this method to add services to the container.
        // For more information on how to configure your application, visit https://go.microsoft.com/fwlink/?LinkID=398940
        public void ConfigureServices(IServiceCollection services)
        {
            var storage = new RedisStorage("127.0.0.1:6379,defaultDatabase=1,poolsize=50", new RedisStorageOptions
            {
                Prefix = "hangfire.dev:"
            });
            services.AddHangfire(o => { o.UseStorage(storage); });
            services.AddHangfireServer((sp) =>
            {
                sp.Queues = new[] { "dev", "test", "pred", "prod", "default" };
            });
            JobStorage.Current = storage;

        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
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
            app.UseHangfireDashboard(options: new DashboardOptions
            {
                IgnoreAntiforgeryToken = true,
                DisplayStorageConnectionString = false, // 是否显示数据库连接信息
                IsReadOnlyFunc = context => false,
            });
            //app.UseHangfireServer(new BackgroundJobServerOptions
            //{
            //    Queues = new []{"dev","test","pred","prod","default"}
            //});
            RecurringJob.AddOrUpdate(() => Console.WriteLine($"输出内容：{DateTime.Now:yyyy-MM-dd HH:mm:ss.sss}"), "*/1 * * * * ? ", TimeZoneInfo.Local);
            for (var i = 0; i <= 50; i++)
                BackgroundJob.Schedule(() => Console.WriteLine($"测试延时任务-输出内容：{DateTime.Now:yyyy-MM-dd HH:mm:ss.sss}"), TimeSpan.FromMinutes(1 + i));

        }
    }
}
