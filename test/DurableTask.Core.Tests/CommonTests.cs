//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

namespace DurableTask.Core.Tests
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Utility;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using DurableTask.Core.Common;
    using DurableTask.Core.Tracing;
    using Newtonsoft.Json;

    [TestClass]
    public class CommonTests
    {
        [TestMethod]
        public void DateTimeExtensionsIsSetTest()
        {
            Assert.IsTrue(DateTime.Now.IsSet());
            Assert.IsTrue(DateTime.MaxValue.IsSet());
            Assert.IsFalse(DateTime.MinValue.IsSet());
            Assert.IsFalse(DateTimeUtils.MinDateTime.IsSet());

            if (DateTimeUtils.MinDateTime == DateTime.FromFileTimeUtc(0))
            {
                Assert.IsFalse(DateTime.FromFileTimeUtc(0).IsSet());
            }
        }

        /// <summary>
        /// Test that the SerializeToJson utility ignores the default/global serialization config.
        /// This ensures that users may not influence our own serialization by setting modifying global settings.
        /// </summary>
        [TestMethod]
        public void DefaultJsonConvertSettingsAreIgnored()
        {
            // set default Newtonsoft.JSON settings to drop default values (i.e 0 for numbers)
            var previousDefaultSettings = JsonConvert.DefaultSettings;

            JsonSerializerSettings globalSettings = new()
            {
                DefaultValueHandling = DefaultValueHandling.Ignore,
            };

            try
            {
                JsonConvert.DefaultSettings = () => globalSettings;

                // create object to serialize. Set EventId to 0 so it may get dropped
                var messageData = new TestUtils.DummyMessage(eventId: 0, "payload");

                // serialize with global settings, and then with the serialization utility
                var jsonStrFromGlobalSettings = JsonConvert.SerializeObject(messageData);
                var jsonStrFromUtils = Utils.SerializeToJson(messageData);

                // global serializer is expected to drop EventId, but the utility serializer doesn't.
                Assert.IsFalse(jsonStrFromGlobalSettings.Contains("EventId"));
                Assert.IsTrue(jsonStrFromUtils.Contains("EventId"));
            }
            finally
            {
                // restore default settings
                JsonConvert.DefaultSettings = previousDefaultSettings;
            }

        }

        [TestMethod]
        public void ShouldValidateEventSource()
        {
            EventSourceAnalyzer.InspectAll(DefaultEventSource.Log);
        }

        /// <summary>
        /// Ignores the action since number of attempts is zero
        /// </summary>
        /// <returns></returns>
        [TestMethod]
        public async Task ExecuteWithRetryIsNoOpBecauseNoAttemptsWereMade()
        {
            const int NumberOfAttempts = 0;
            var retries = 0;

            // Call the method overload which signature does not return any value
            await Utils.ExecuteWithRetries(() =>
            {
                retries++;
                return Task.Delay(1);
            }, string.Empty, string.Empty, NumberOfAttempts, 0);

            Assert.AreEqual(NumberOfAttempts, retries, "Action was executed when should have been ignored");

            // Call the method overload which signature does return a value
            bool hasSucceed = await Utils.ExecuteWithRetries(() =>
                Task.FromResult(true), string.Empty, string.Empty, NumberOfAttempts, 0);

            Assert.AreEqual(NumberOfAttempts, retries, "Action was executed when should have been ignored");
            Assert.IsFalse(hasSucceed, "Retry logic executed the action when should have been ignored");
        }

        /// <summary>
        /// Executes the action with retry and rethrows the exception.
        /// </summary>
        /// <returns></returns>
        [TestMethod]
        public async Task ExecuteWithRetryThrowsByRetryCountLimit()
        {
            const int NumberOfAttempts = 3;
            var retries = 0;
            var hasFailed = false;

            try
            {
                // Call the method overload which signature does not return any value
                await Utils.ExecuteWithRetries(() =>
                {
                    // ReSharper disable once AccessToModifiedClosure (Intentional...)
                    retries++;

                    throw new DivideByZeroException();
                }, string.Empty, string.Empty, NumberOfAttempts, 0);
            }
            catch (DivideByZeroException)
            {
                hasFailed = true;
            }

            Assert.AreEqual(NumberOfAttempts, retries, "Number of attempts not honored by the retry logic");
            Assert.IsTrue(hasFailed, "Retry logic didn't unwind and rethrow inner exception");

            retries = 0;
            hasFailed = false;
            try
            {
                // Call the method overload which signature returns a value
                await Utils.ExecuteWithRetries<bool>(() =>
                {
                    retries++;
                    throw new DivideByZeroException();
                }, string.Empty, string.Empty, NumberOfAttempts, 0);
            }
            catch (DivideByZeroException)
            {
                hasFailed = true;
            }

            Assert.AreEqual(NumberOfAttempts, retries, "Number of attempts not honored by the retry logic");
            Assert.IsTrue(hasFailed, "Retry logic didn't unwind and rethrow inner exception");
        }

        /// <summary>
        /// Executes the action with retry until it succeeds.
        /// </summary>
        /// <returns></returns>
        [TestMethod]
        public async Task ExecuteWithRetrySucceedsAfterXAttemptsMinus1()
        {
            const int NumberOfAttempts = 5;

            for (var attemptsBeforeFailure = 1; attemptsBeforeFailure < 5; attemptsBeforeFailure++)
            {
                var retries = 0;
                var hasSucceed = false;

                // Call the method overload which signature does not return any value
                await Utils.ExecuteWithRetries(() =>
                {
                    retries++;

                    // ReSharper disable once AccessToModifiedClosure
                    if (retries < attemptsBeforeFailure)
                    {
                        throw new DivideByZeroException();
                    }

                    hasSucceed = true;
                    return Task.Delay(1);
                }, string.Empty, string.Empty, NumberOfAttempts, 0);

                Assert.AreEqual(attemptsBeforeFailure, retries, "Number of attempts not honored by the retry logic");
                Assert.IsTrue(hasSucceed, "Retry logic throws an exception when should succeed");
            }

            for (var attemptsBeforeFailure = 1; attemptsBeforeFailure < 5; attemptsBeforeFailure++)
            {
                var retries = 0;

                // Call the method overload which signature does return any value
                bool hasSucceed = await Utils.ExecuteWithRetries(() =>
                {
                    retries++;

                    // ReSharper disable once AccessToModifiedClosure
                    if (retries < attemptsBeforeFailure)
                    {
                        throw new DivideByZeroException();
                    }

                    return Task.FromResult(true);
                }, string.Empty, string.Empty, NumberOfAttempts, 0);

                Assert.AreEqual(attemptsBeforeFailure, retries, "Number of attempts not honored by the retry logic");
                Assert.IsTrue(hasSucceed, "Retry logic throws an exception when should succeed");
            }
        }
    }
}