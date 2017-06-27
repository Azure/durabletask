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
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    class Program
    {
        static void Main(string[] args)
        {
            MainAsync(args).Wait();
        }

        static async Task MainAsync(string[] args)
        {
            var timeToRun = ValidateArgs(args);

            var tasks = new List<Task>();
            if (timeToRun != TimeSpan.Zero)
            {
                CancellationTokenSource cts = new CancellationTokenSource();
                if (timeToRun <= TimeSpan.FromMinutes(Int32.MaxValue)) //Cancellation token can't take more time than Int32.MaxValue
                {
                    cts.CancelAfter(timeToRun);
                }

                Console.WriteLine("Press CTRL+C to stop running orchestrations and print results");
                Console.CancelKeyPress += (sender, e) =>
                {
                    cts.Cancel();
                    e.Cancel = true;
                };

                var runner = new OrchestrationRunner();
                tasks.Add(runner.RunTestAsync(cts.Token));

                await Task.Delay(TimeSpan.FromMinutes(2));

                var chaosRunner = new ChaosRunner();
                tasks.Add(chaosRunner.RunFailoverScenario(timeToRun, cts.Token));
            }

            await Task.WhenAll(tasks);
        }

        static TimeSpan ValidateArgs(string[] args)
        {
            if (args == null || args.Length == 0)
            {
                return TimeSpan.MaxValue; //Indicates that test should run till user stops it.
            }

            var first = args[0];
            int toBeRunTime = 0;
            if (!int.TryParse(first, out toBeRunTime))
            {
                Console.WriteLine($"{Process.GetCurrentProcess().ProcessName} [Time to run (minutes)]");
            }

            return TimeSpan.FromMinutes(toBeRunTime);
        }
    }
}
