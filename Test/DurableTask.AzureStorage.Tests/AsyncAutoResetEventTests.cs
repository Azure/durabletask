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

namespace DurableTask.AzureStorage.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class AsyncAutoResetEventTests
    {
        [DataTestMethod]
        [DataRow(false, false)]
        [DataRow(true, true)]
        public async Task InitialState(bool initiallySignaled, bool expectedResult)
        {
            var resetEvent = new AsyncAutoResetEvent(initiallySignaled);
            bool result = await resetEvent.WaitAsync(TimeSpan.Zero);
            Assert.AreEqual<bool>(result, expectedResult);
        }

        [TestMethod]
        public async Task SignalSingleWaiter()
        {
            var resetEvent = new AsyncAutoResetEvent(signaled: false);
            Task<bool> waiter = resetEvent.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.IsFalse(waiter.IsCompleted);
            resetEvent.Set();
            Task winner = await Task.WhenAny(waiter, Task.Delay(TimeSpan.FromSeconds(2)));
            Assert.IsTrue(waiter == winner, "waiter is supposed to be the winner");
            Assert.IsTrue(waiter.IsCompleted, "waiter.IsCompleted should be true");
            Assert.IsTrue(waiter.Result, "waiter.Result should be true");
        }

        [TestMethod]
        public async Task SignalMultipleWaiters()
        {
            var resetEvent = new AsyncAutoResetEvent(signaled: false);

            var waiters = new List<Task<bool>>();
            for (int i = 0; i < 10; i++)
            {
                Task<bool> waiter = resetEvent.WaitAsync(TimeSpan.FromSeconds(60));
                waiters.Add(waiter);
            }

            for (int i = 0; i < waiters.Count; i++)
            {
                resetEvent.Set();

                // Sleep to let the signaling happen
                await Task.Delay(200);

                for (int j = i; j < waiters.Count; j++)
                {
                    Task<bool> waiter = waiters[j];
                    Assert.AreEqual(i == j, waiter.IsCompleted && waiter.Result);
                }
            }
        }
    }
}
