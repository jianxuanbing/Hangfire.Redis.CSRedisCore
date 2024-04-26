using System;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Redis.States;

/// <summary>
/// 已失败状态处理器
/// </summary>
internal class FailedStateHandler : IStateHandler
{
    /// <summary>
    /// 应用
    /// </summary>
    /// <param name="context">应用状态上下文</param>
    /// <param name="transaction">事务</param>
    public void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction) => transaction.AddToSet(Const.Failed, context.BackgroundJob.Id, JobHelper.ToTimestamp(DateTime.UtcNow));

    /// <summary>
    /// 取消应用
    /// </summary>
    /// <param name="context">应用状态上下文</param>
    /// <param name="transaction">事务</param>
    public void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction) => transaction.RemoveFromSet(Const.Failed, context.BackgroundJob.Id);

    /// <summary>
    /// 状态名
    /// </summary>
    public string StateName => FailedState.StateName;
}