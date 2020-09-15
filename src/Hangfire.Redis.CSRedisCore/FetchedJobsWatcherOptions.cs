using System;

namespace Hangfire.Redis
{
    /// <summary>
    /// 拉取作业观察者选项配置
    /// </summary>
    internal class FetchedJobsWatcherOptions
    {
        /// <summary>
        /// 初始化一个<see cref="FetchedJobsWatcherOptions"/>类型的实例
        /// </summary>
        public FetchedJobsWatcherOptions()
        {
            FetchedLockTimeout = TimeSpan.FromMinutes(1);
            CheckedTimeout = TimeSpan.FromMinutes(1);
            SleepTimeout = TimeSpan.FromMinutes(1);
        }

        /// <summary>
        /// 拉取锁定超时时间
        /// </summary>
        public TimeSpan FetchedLockTimeout { get; set; }

        /// <summary>
        /// 检查超时时间
        /// </summary>
        public TimeSpan CheckedTimeout { get; set; }

        /// <summary>
        /// 睡眠超时时间
        /// </summary>
        public TimeSpan SleepTimeout { get; set; }
    }
}
