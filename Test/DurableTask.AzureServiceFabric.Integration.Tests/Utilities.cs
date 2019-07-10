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

namespace DurableTask.AzureServiceFabric.Integration.Tests
{
    using System;
    using System.Diagnostics;
    using System.Net.Http;
    using System.Threading.Tasks;

    using DurableTask.Core;
    using DurableTask.AzureServiceFabric.Remote;
    using DurableTask.AzureServiceFabric.Service;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    public static class Utilities
    {
        public static TaskHubClient CreateTaskHubClient()
        {
            var partitionProvider = new FabricPartitionEndpointResolver(new Uri(Constants.TestFabricApplicationAddress), new DefaultStringPartitionHashing());
            var httpClient = new HttpClient();
            return new TaskHubClient(new RemoteOrchestrationServiceClient(partitionProvider));
        }

        public static async Task ThrowsException<TException>(Func<Task> action, string expectedMessage) where TException : Exception
        {
            try
            {
                await action();
                Assert.Fail($"Method {action.Method} did not throw the expected exception of type {typeof(TException).Name}");
            }
            catch (Exception ex)
            {
                AggregateException aggregate = ex as AggregateException;
                if (aggregate != null)
                {
                    ex = aggregate.InnerException;
                }

                TException expected = ex as TException;
                if (expected == null)
                {
                    Assert.Fail($"Method {action.Method} is expected to throw exception of type {typeof(TException).Name} but has thrown {ex.GetType().Name} instead.");
                }
                else if (!string.Equals(expected.Message, expectedMessage, StringComparison.Ordinal))
                {
                    Assert.Fail($"Method {action.Method} is expected to throw exception with message '{expectedMessage}' but has thrown the message '{expected.Message}' instead.");
                }
            }
        }

        public static async Task<TimeSpan> MeasureAsync(Func<Task> asyncAction)
        {
            var timer = Stopwatch.StartNew();
            await asyncAction();
            timer.Stop();
            return timer.Elapsed;
        }
    }
}
