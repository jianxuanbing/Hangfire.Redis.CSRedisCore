using System;
using Xunit;

namespace Hangfire.Redis.Tests
{
    public class RedisStorageOptionsFacts
    {

        [Fact]
        public void InvisibilityTimeout_HasDefaultValue()
        {
            var options = CreateOptions();
            Assert.Equal(TimeSpan.FromMinutes(30), options.InvisibilityTimeout);
        }

        [Fact]
        public void Db_HasDefaultValue()
        {
            var options = CreateOptions();
            var property = typeof(RedisStorageOptions).GetProperty("Db");

            Assert.Equal(0, (int)property.GetValue(options));
        }

        [Fact]
        public void Db_IsMarkedObsolete_BecauseDatabaseSelectionComesFromConnection()
        {
            var property = typeof(RedisStorageOptions).GetProperty("Db");
            var attribute = (ObsoleteAttribute)Attribute.GetCustomAttribute(property, typeof(ObsoleteAttribute));

            Assert.NotNull(attribute);
            Assert.Contains("defaultDatabase", attribute.Message);
        }

        private static RedisStorageOptions CreateOptions() => new RedisStorageOptions();
    }
}
