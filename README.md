# Hangfire.Redis.CSRedisCore

`Hangfire.Redis.CSRedisCore` 是基于 `CSRedisCore` 的 Hangfire Redis Storage Provider。

当前版本已完成 Hangfire.Core 1.8 基础兼容：

- 保持 `netstandard2.0`
- 支持 Hangfire 1.8 的 Job Queue Property
- 支持 `Connection.GetUtcDateTime`
- 保持现有 Redis key 结构兼容
- 旧任务缺少 `Queue` 字段时仍可读取

## 安装

```powershell
dotnet add package Hangfire.Redis.CSRedisCore
```

## 基本配置

```csharp
var redisClient = new CSRedisClient("127.0.0.1:6379,defaultDatabase=1,poolsize=50");

GlobalConfiguration.Configuration
    .UseRedisStorage(redisClient, new RedisStorageOptions
    {
        Prefix = "{hangfire}:"
    });
```

## Hangfire 1.8 兼容级别

升级建议分两阶段进行。

第一阶段，集群内仍存在旧版 Server 时：

```csharp
GlobalConfiguration.Configuration
    .SetDataCompatibilityLevel(CompatibilityLevel.Version_170)
    .UseRedisStorage(redisClient, new RedisStorageOptions
    {
        Prefix = "{hangfire}:"
    });
```

第二阶段，所有 Server 都升级完成后：

```csharp
GlobalConfiguration.Configuration
    .SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
    .UseRedisStorage(redisClient, new RedisStorageOptions
    {
        Prefix = "{hangfire}:"
    });
```

## Queue 支持

Hangfire 1.8 已支持 Job 级别的 `Queue` 属性，本存储已支持该字段的持久化和读取。

默认队列示例：

```csharp
BackgroundJob.Enqueue(() => Console.WriteLine("default queue"));
```

如果项目使用自定义队列，请同时配置 Server 的 `Queues`：

```csharp
app.UseHangfireServer(new BackgroundJobServerOptions
{
    Queues = new[] { "critical", "default" }
});
```

## CSRedisCore 配置说明

- Redis 连接由 `CSRedisClient` 管理
- Redis database 建议在连接串中指定，例如 `defaultDatabase=1`
- `RedisStorageOptions.Db` 当前不参与 `CSRedisClient` 的数据库选择
- Hangfire Key Prefix 建议使用 `"{hangfire}:"`
- Redis Cluster 下不要破坏 `{hangfire}` hash tag
- 不建议同时配置 CSRedisCore 全局 prefix 和 Hangfire Prefix

## Prefix 说明

- 默认 `Prefix` 为 `"{hangfire}:"`
- 升级到 Hangfire 1.8 不需要修改现有 Hangfire Redis key
- 新增的 `Queue` 信息仅写入现有 `job:{id}` Hash 的可选字段 `Queue`

## 示例

```csharp
var storage = new RedisStorage(redisClient, new RedisStorageOptions
{
    Prefix = "{hangfire}:"
});

services.AddHangfire(configuration => configuration
    .SetDataCompatibilityLevel(CompatibilityLevel.Version_170)
    .UseStorage(storage));

services.AddHangfireServer(options =>
{
    options.Queues = new[] { "critical", "default" };
});
```

## 测试

测试默认连接本地 Redis：

- Host: `127.0.0.1:6379`
- Database: `1`

运行命令：

```powershell
dotnet test .\Hangfire.Redis.CSRedisCore.sln
```

## 已支持的 Hangfire 1.8 能力

- `Storage.ExtendedApi`
- `Connection.GetUtcDateTime`
- `Job.Queue`

以下能力当前未声明支持：

- `Transaction.CreateJob`
- `Transaction.SetJobParameter`
- `Transaction.RemoveFromQueue`
- `Transaction.AcquireDistributedLock`
- `Monitoring.DeletedStateGraphs`
- `Monitoring.AwaitingJobs`

这些能力会在具备真实实现后再开启。
