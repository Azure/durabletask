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

namespace DurableTask.AzureServiceFabric.Tests
{
    using System;

    using DurableTask.AzureServiceFabric.TaskHelpers;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class CountBasedFixedDelayRetryPolicyTests
    {
        [TestMethod]
        public void CountBasedFixedDelayRetry_Attempts_MaxCount_Times()
        {
            var testRetryPolicy = new CountBasedFixedDelayRetryPolicy(3, TimeSpan.Zero);
            int attempts = 0;
            while (testRetryPolicy.ShouldExecute())
            {
                attempts++;
            }
            Assert.AreEqual(3, attempts);
        }

        [TestMethod]
        public void CountBasedFixedDelayRetry_Last_Retry_Delay_Is_Zero()
        {
            var testRetryPolicy = new CountBasedFixedDelayRetryPolicy(1, TimeSpan.FromSeconds(1));
            TimeSpan lastDelay = TimeSpan.MaxValue;
            while (testRetryPolicy.ShouldExecute())
            {
                lastDelay = testRetryPolicy.GetNextDelay();
            }
            Assert.AreEqual(TimeSpan.Zero, lastDelay);
        }

        [TestMethod]
        public void CountBasedFixedDelayRetry_Non_Last_Retry_Delay_Is_Fixed()
        {
            TimeSpan fixedDelay = TimeSpan.FromSeconds(23);
            var testRetryPolicy = new CountBasedFixedDelayRetryPolicy(3, fixedDelay);
            int attemptNumber = 1;
            TimeSpan nextDelay = TimeSpan.MaxValue;
            while (testRetryPolicy.ShouldExecute())
            {
                nextDelay = testRetryPolicy.GetNextDelay();
                if (attemptNumber < 3)
                {
                    Assert.AreEqual(fixedDelay, nextDelay);
                }
                attemptNumber++;
            }
            Assert.AreEqual(TimeSpan.Zero, nextDelay);
        }
    }
}
