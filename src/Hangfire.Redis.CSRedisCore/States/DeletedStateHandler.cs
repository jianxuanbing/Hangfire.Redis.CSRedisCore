using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Redis.States
{
    /// <summary>
    /// 已删除状态处理器
    /// </summary>
    internal class DeletedStateHandler : IStateHandler
    {
        /// <summary>
        /// 应用
        /// </summary>
        /// <param name="context">应用状态上下文</param>
        /// <param name="transaction">事务</param>
        public void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.InsertToList(Const.Deleted, context.BackgroundJob.Id);
            if (context.Storage is RedisStorage storage && storage.DeletedListSize > 0)
                transaction.TrimList(Const.Deleted, 0, storage.DeletedListSize);
        }

        /// <summary>
        /// 取消应用
        /// </summary>
        /// <param name="context">应用状态上下文</param>
        /// <param name="transaction">事务</param>
        public void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction) => transaction.RemoveFromList(Const.Deleted,context.BackgroundJob.Id);

        /// <summary>
        /// 状态名
        /// </summary>
        public string StateName => DeletedState.StateName;
    }
}
