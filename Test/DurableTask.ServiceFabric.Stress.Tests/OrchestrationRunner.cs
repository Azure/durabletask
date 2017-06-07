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

namespace DurableTask.ServiceFabric.Failover.Tests
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Test.Orchestrations.Perf;
    using Microsoft.ServiceFabric.Services.Client;
    using Microsoft.ServiceFabric.Services.Remoting.Client;
    using TestApplication.Common;

    class OrchestrationRunner
    {
        ConcurrentBag<OrchestrationInstance> instances = new ConcurrentBag<OrchestrationInstance>();
        IRemoteClient serviceClient = ServiceProxy.Create<IRemoteClient>(new Uri("fabric:/TestFabricApplication/TestStatefulService"), new ServicePartitionKey(1));
        Dictionary<string, long> outcomeFrequencies = new Dictionary<string, long>();

        public async Task RunTestAsync(CancellationToken token)
        {
            CancellationTokenSource cts2 = new CancellationTokenSource();

            Stopwatch watch = Stopwatch.StartNew();
            var programTask = RunOrchestrations(token);
            var statusTask = PollState(cts2.Token);

            await programTask;

            cts2.Cancel();
            await statusTask;
            watch.Stop();

            Func<TimeSpan, string> elapsedTimeFormatter = timeSpan => $"{timeSpan.Hours:00}:{timeSpan.Minutes:00}:{timeSpan.Seconds:00}.{timeSpan.Milliseconds / 10:00}";
            Console.WriteLine($"{nameof(RunTestAsync)}: Total elapsed time for the program : {elapsedTimeFormatter(watch.Elapsed)}");
        }

        async Task PollState(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await PrintStatusAggregation();
                await Task.Delay(TimeSpan.FromSeconds(10));
            }
            await PrintStatusAggregation();
        }

        async Task PrintStatusAggregation()
        {
            Console.WriteLine($"{nameof(PrintStatusAggregation)}: Number of instances so far : {instances.Count}");
            foreach (var instance in instances)
            {
                var state = await Utils.ExceptionWrapper(() => serviceClient.GetOrchestrationState(instance), $"Get Orchestration State {instance}");
                var key = state != null ? state.OrchestrationStatus.ToString() : "NullState";

                if (!outcomeFrequencies.ContainsKey(key))
                {
                    outcomeFrequencies.Add(key, 0);
                }

                outcomeFrequencies[key]++;
            }

            foreach (var kvp in outcomeFrequencies)
            {
                Console.WriteLine($"{nameof(PrintStatusAggregation)}: {kvp.Key} : {kvp.Value}");
            }

            outcomeFrequencies.Clear();
        }

        async Task RunOrchestrations(CancellationToken cancellationToken)
        {
            int totalRequests = 0;
            List<Task> tasks = new List<Task>();
            Console.WriteLine($"{nameof(RunOrchestrations)}: Starting orchestrations");

            while (!cancellationToken.IsCancellationRequested)
            {
                var newOrchData = new TestOrchestrationData()
                {
                    NumberOfParallelTasks = 15,
                    NumberOfSerialTasks = 5,
                    MaxDelay = 500,
                    MinDelay = 50,
                    DelayUnit = TimeSpan.FromMilliseconds(1),
                    UseTimeoutTask = false, //Change to true when testing timers
                    ExecutionTimeout = TimeSpan.FromMinutes(5)
                };

                totalRequests++;
                var instance = await Utils.ExceptionWrapper(() => serviceClient.StartTestOrchestrationAsync(newOrchData), $"StartOrchestration {totalRequests}");
                if (instance != null)
                {
                    instances.Add(instance);
                    var waitTask = Utils.ExceptionWrapper(() => serviceClient.WaitForOrchestration(instance, TimeSpan.FromMinutes(10)), $"WaitForOrchestration {totalRequests}");
                    tasks.Add(waitTask);
                }

                var delayBetweenRequests = 50;
                try
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(delayBetweenRequests), cancellationToken);
                }
                catch (TaskCanceledException)
                {
                    break;
                }
            }

            Console.WriteLine($"{nameof(RunOrchestrations)}: Total orchestrations : {totalRequests}");
            Console.WriteLine($"{nameof(RunOrchestrations)}: Waiting for pending orchestrations");
            await Task.WhenAll(tasks);

            Console.WriteLine($"{nameof(RunOrchestrations)}: Done");
        }

        int GetVariableDelayForTotalRequests(int totalRequests)
        {
            int delay = 0;
            if (totalRequests % 10 == 0)
            {
                delay = 1000;
            }
            if (totalRequests % 100 == 0)
            {
                delay = 3000;
            }
            return delay;
        }
    }
}
