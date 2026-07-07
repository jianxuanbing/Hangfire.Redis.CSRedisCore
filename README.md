# Hangfire.Redis.CSRedisCore

`Hangfire.Redis.CSRedisCore` 是基于 `CSRedisCore` 的 Hangfire Redis Storage Provider，当前仓库只支持 `Hangfire.Core 1.8.x`。

## 版本范围

- 主库依赖 `Hangfire.Core [1.8.0,2.0.0)`
- sample / tests 使用 `Hangfire.AspNetCore 1.8.23`
- 不再承诺 Hangfire 1.7 兼容性

## 安装

```powershell
dotnet add package Hangfire.Redis.CSRedisCore
```

## 基本配置

```csharp
var redisClient = new CSRedisClient("127.0.0.1:6379,defaultDatabase=1,poolsize=50");

GlobalConfiguration.Configuration
    .SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
    .UseRedisStorage(redisClient, new RedisStorageOptions
    {
        Prefix = RedisStorageOptions.DefaultPrefix
    });
```

如果所有 server 还没有一起升级到 Hangfire 1.8，请先完成 server 升级，再切换到 `CompatibilityLevel.Version_180`。

## 队列与 key 兼容

- 保持现有 Redis key 结构：`job:{id}`、`queue:{queue}`、`queue:{queue}:dequeued`、`queues`
- `Job.Queue` 作为 `job:{id}` hash 的可选字段写入
- 旧 job hash 没有 `Queue` 字段时仍可读取
- 默认前缀是 `"{hangfire}:"`，保留 Redis Cluster hash tag

## CSRedisCore 配置约束

- Redis database 必须通过 `CSRedisClient` / 连接串控制，例如 `defaultDatabase=1`
- `RedisStorageOptions.Db` 已废弃，不参与数据库选择
- 不要同时配置 CSRedisCore 全局 prefix 和 Hangfire prefix

## Hangfire 1.8 feature matrix

当前已支持：

- `Storage.ExtendedApi`
- `Connection.BatchedGetFirstByLowestScoreFromSet`
- `Connection.GetUtcDateTime`
- `Job.Queue`
- `Transaction.CreateJob`
- `Transaction.SetJobParameter`
- `Transaction.RemoveFromQueue(typeof(RedisFetchedJob))`

当前明确不支持：

- `Transaction.AcquireDistributedLock`
- `Monitoring.DeletedStateGraphs`
- `Monitoring.AwaitingJobs`

说明：本 provider 的 write transaction 基于 `CSRedisCore` 的 pipelined write 路径，不提供可回滚的 Redis 事务语义，因此只开启了已经有真实实现和测试覆盖的 Hangfire 1.8 feature。

## Sample

sample 项目位于 [samples/Hangfire.Redis.Sample/Program.cs](/e:/Bing_Framework/Hangfire.Redis.CSRedisCore/samples/Hangfire.Redis.Sample/Program.cs:1) 和 [samples/Hangfire.Redis.Sample/Startup.cs](/e:/Bing_Framework/Hangfire.Redis.CSRedisCore/samples/Hangfire.Redis.Sample/Startup.cs:1)，默认展示：

- `CompatibilityLevel.Version_180`
- `critical` / `default` 两个队列
- 一个 `critical` 启动作业
- 一个默认队列延时作业
- 一个默认队列 recurring 作业

本地运行 sample：

```powershell
dotnet run --project .\samples\Hangfire.Redis.Sample\Hangfire.Redis.Sample.csproj
```

打开 `http://localhost:5000/hangfire`。

## 测试

测试默认连接本地 Redis：

- `127.0.0.1:6379`
- `defaultDatabase=1`

运行命令：

```powershell
dotnet test .\tests\Hangfire.Redis.CSRedisCore.Tests\Hangfire.Redis.CSRedisCore.Tests.csproj
```
