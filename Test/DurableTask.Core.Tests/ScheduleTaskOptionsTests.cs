// ---------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ---------------------------------------------------------------

namespace DurableTask.Core.Tests
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.Collections.Generic;

    [TestClass]
    public class ScheduleTaskOptionsTests
    {
        [TestMethod]
        public void CreateBuilder_ShouldReturnBuilderInstance()
        {
            // Act
            ScheduleTaskOptions.Builder builder = ScheduleTaskOptions.CreateBuilder();

            // Assert
            Assert.IsNotNull(builder);
            Assert.IsInstanceOfType(builder, typeof(ScheduleTaskOptions.Builder));
        }

        [TestMethod]
        public void Build_ShouldCreateInstanceWithNullProperties()
        {
            // Act
            ScheduleTaskOptions options = ScheduleTaskOptions.CreateBuilder().Build();

            // Assert
            Assert.IsNotNull(options);
            Assert.IsNull(options.Tags);
            Assert.IsNull(options.RetryOptions);
        }

        [TestMethod]
        public void WithTags_ShouldSetTagsProperty()
        {
            // Arrange
            Dictionary<string, string> tags = new Dictionary<string, string>
            {
                { "key1", "value1" },
                { "key2", "value2" }
            };

            // Act
            ScheduleTaskOptions options = ScheduleTaskOptions.CreateBuilder()
                .WithTags(tags)
                .Build();

            // Assert
            Assert.IsNotNull(options.Tags);
            Assert.AreEqual(2, options.Tags.Count);
            Assert.AreEqual("value1", options.Tags["key1"]);
            Assert.AreEqual("value2", options.Tags["key2"]);
        }

        [TestMethod]
        public void AddTag_WithNullTags_ShouldInitializeTagsCollection()
        {
            // Act
            ScheduleTaskOptions options = ScheduleTaskOptions.CreateBuilder()
                .AddTag("key1", "value1")
                .Build();

            // Assert
            Assert.IsNotNull(options.Tags);
            Assert.AreEqual(1, options.Tags.Count);
            Assert.AreEqual("value1", options.Tags["key1"]);
        }

        [TestMethod]
        public void AddTag_WithExistingTags_ShouldAddToCollection()
        {
            // Arrange
            Dictionary<string, string> tags = new Dictionary<string, string>
            {
                { "key1", "value1" }
            };

            // Act
            ScheduleTaskOptions options = ScheduleTaskOptions.CreateBuilder()
                .WithTags(tags)
                .AddTag("key2", "value2")
                .Build();

            // Assert
            Assert.IsNotNull(options.Tags);
            Assert.AreEqual(2, options.Tags.Count);
            Assert.AreEqual("value1", options.Tags["key1"]);
            Assert.AreEqual("value2", options.Tags["key2"]);
        }

        [TestMethod]
        public void AddTag_OverwriteExistingKey_ShouldUpdateValue()
        {
            // Act
            ScheduleTaskOptions options = ScheduleTaskOptions.CreateBuilder()
                .AddTag("key1", "originalValue")
                .AddTag("key1", "newValue")
                .Build();

            // Assert
            Assert.IsNotNull(options.Tags);
            Assert.AreEqual(1, options.Tags.Count);
            Assert.AreEqual("newValue", options.Tags["key1"]);
        }

        [TestMethod]
        public void WithRetryOptions_Instance_ShouldSetRetryOptionsProperty()
        {
            // Arrange
            RetryOptions retryOptions = new RetryOptions(TimeSpan.FromSeconds(5), 3);

            // Act
            ScheduleTaskOptions options = ScheduleTaskOptions.CreateBuilder()
                .WithRetryOptions(retryOptions)
                .Build();

            // Assert
            Assert.IsNotNull(options.RetryOptions);
            Assert.AreEqual(TimeSpan.FromSeconds(5), options.RetryOptions.FirstRetryInterval);
            Assert.AreEqual(3, options.RetryOptions.MaxNumberOfAttempts);
        }

        [TestMethod]
        public void WithRetryOptions_Parameters_ShouldCreateAndSetRetryOptions()
        {
            // Act
            ScheduleTaskOptions options = ScheduleTaskOptions.CreateBuilder()
                .WithRetryOptions(TimeSpan.FromSeconds(5), 3)
                .Build();

            // Assert
            Assert.IsNotNull(options.RetryOptions);
            Assert.AreEqual(TimeSpan.FromSeconds(5), options.RetryOptions.FirstRetryInterval);
            Assert.AreEqual(3, options.RetryOptions.MaxNumberOfAttempts);
            Assert.AreEqual(1, options.RetryOptions.BackoffCoefficient);
            Assert.AreEqual(TimeSpan.MaxValue, options.RetryOptions.MaxRetryInterval);
        }

        [TestMethod]
        public void WithRetryOptions_WithConfigureAction_ShouldConfigureRetryOptions()
        {
            // Act
            ScheduleTaskOptions options = ScheduleTaskOptions.CreateBuilder()
                .WithRetryOptions(TimeSpan.FromSeconds(5), 3, retryOptions =>
                {
                    retryOptions.BackoffCoefficient = 2.0;
                    retryOptions.MaxRetryInterval = TimeSpan.FromMinutes(1);
                })
                .Build();

            // Assert
            Assert.IsNotNull(options.RetryOptions);
            Assert.AreEqual(TimeSpan.FromSeconds(5), options.RetryOptions.FirstRetryInterval);
            Assert.AreEqual(3, options.RetryOptions.MaxNumberOfAttempts);
            Assert.AreEqual(2.0, options.RetryOptions.BackoffCoefficient);
            Assert.AreEqual(TimeSpan.FromMinutes(1), options.RetryOptions.MaxRetryInterval);
        }

        [TestMethod]
        public void WithRetryOptions_PassNullConfigureAction_ShouldStillCreateRetryOptions()
        {
            // Act
            ScheduleTaskOptions options = ScheduleTaskOptions.CreateBuilder()
                .WithRetryOptions(TimeSpan.FromSeconds(5), 3, null)
                .Build();

            // Assert
            Assert.IsNotNull(options.RetryOptions);
            Assert.AreEqual(TimeSpan.FromSeconds(5), options.RetryOptions.FirstRetryInterval);
            Assert.AreEqual(3, options.RetryOptions.MaxNumberOfAttempts);
        }

        [TestMethod]
        public void FluentInterface_CombiningAllMethods_ShouldBuildCorrectInstance()
        {
            // Arrange
            RetryOptions retryOptions = new RetryOptions(TimeSpan.FromSeconds(1), 2);

            // Act
            ScheduleTaskOptions options = ScheduleTaskOptions.CreateBuilder()
                .AddTag("env", "test")
                .WithRetryOptions(retryOptions)
                .AddTag("priority", "high")
                .Build();

            // Assert
            Assert.IsNotNull(options);
            Assert.IsNotNull(options.Tags);
            Assert.AreEqual(2, options.Tags.Count);
            Assert.AreEqual("test", options.Tags["env"]);
            Assert.AreEqual("high", options.Tags["priority"]);
            Assert.IsNotNull(options.RetryOptions);
            Assert.AreEqual(retryOptions.FirstRetryInterval, options.RetryOptions.FirstRetryInterval);
            Assert.AreEqual(retryOptions.MaxNumberOfAttempts, options.RetryOptions.MaxNumberOfAttempts);
        }
    }
}