using System.Collections.Generic;
using System.Linq;
using CSRedis;

namespace Hangfire.Redis
{
    /// <summary>
    /// Redis数据库扩展
    /// </summary>
    internal static class RedisDatabaseExtensions
    {
        /// <summary>
        /// 将字典转对象数组
        /// </summary>
        /// <param name="dic">字典</param>
        public static object[] DicToObjectArray(this IEnumerable<KeyValuePair<string, string>> dic)
        {
            var count = dic.Count();
            var obj = new object[count * 2];
            for (var i = 0; i < count; i++)
            {
                var ele = dic.ElementAt(i);
                obj[i * 2] = ele.Key;
                obj[i * 2 + 1] = ele.Value;
            }
            return obj;
        }

        /// <summary>
        /// 获取值映射字典
        /// </summary>
        /// <param name="redis">Redis</param>
        /// <param name="keys">缓存键数组</param>
        public static Dictionary<string, string> GetValuesMap(this CSRedisClient redis, string[] keys)
        {
            var valuesArr = redis.MGet(keys);
            var result = new Dictionary<string, string>(valuesArr.Length);
            for (var i = 0; i < valuesArr.Length; i++)
                result.Add(keys[i], valuesArr[i]);
            return result;
        }
    }
}
