using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Redis.States
{
    /// <summary>
    /// 已成功状态处理器
    /// </summary>
    internal class SucceededStateHandler : IStateHandler
    {
        /// <summary>
        /// 应用
        /// </summary>
        /// <param name="context">应用状态上下文</param>
        /// <param name="transaction">事务</param>
        public void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.InsertToList(Const.Succeeded, context.BackgroundJob.Id);
            if (context.Storage is RedisStorage storage && storage.SucceededListSize > 0)
                transaction.TrimList(Const.Succeeded, 0, storage.SucceededListSize);
        }

        /// <summary>
        /// 取消应用
        /// </summary>
        /// <param name="context">应用状态上下文</param>
        /// <param name="transaction">事务</param>
        public void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction) => transaction.RemoveFromList(Const.Succeeded, context.BackgroundJob.Id);

        /// <summary>
        /// 状态名
        /// </summary>
        public string StateName => SucceededState.StateName;
    }
}
