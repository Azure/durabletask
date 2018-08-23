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

namespace DurableTask.ServiceFabric.Tests.Tooling
{
    using System;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using Microsoft.ServiceFabric.Services.Client;
    using Microsoft.ServiceFabric.Services.Remoting.Client;
    using TestApplication.Common;

    class Program
    {
        static IRemoteClient serviceClient = ServiceProxy.Create<IRemoteClient>(new Uri("fabric:/TestFabricApplication/TestStatefulService"), new ServicePartitionKey(1));

        static void Main(string[] args)
        {
            MainAsync().Wait();
        }

        static void PrintMenu()
        {
            Console.WriteLine("-----------------------------------");
            Console.WriteLine("        1. List Orchestrations");
            Console.WriteLine("        2. Get runtime state");
            Console.WriteLine("        3. Terminate instance");
            Console.WriteLine("-----------------------------------");
        }

        static string Prompt(string message)
        {
            Console.WriteLine(message);
            return Console.ReadLine();
        }

        static async Task MainAsync()
        {
            while (true)
            {
                try
                {
                    PrintMenu();
                    var action = Prompt("Type an option or hit enter to exit");

                    switch (action)
                    {
                        case "1":
                            await ListOrchestrations();
                            break;
                        case "2":
                            await GetOrchestrationRuntimeState();
                            break;
                        case "3":
                            await TerminateOrchestration();
                            break;
                        default:
                            return;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Unhandled exception caught from the operation : {ex}");
                }
            }
        }

        private static async Task TerminateOrchestration()
        {
            var instanceId = Prompt("InstanceId?");
            var executionId = Prompt("ExecutionId?");
            var instance = new OrchestrationInstance() { InstanceId = instanceId, ExecutionId = executionId };
            await serviceClient.TerminateOrchestration(instanceId, executionId);
            var state = await serviceClient.WaitForOrchestration(instance, TimeSpan.FromMinutes(1));
            if (state == null)
            {
                Console.WriteLine("Orchestration did not complete within 1 minute after termination, trying to get current state");
                state = await serviceClient.GetOrchestrationState(instance);
                Console.WriteLine($"Current status : {state?.OrchestrationStatus}");
            }
        }

        private static async Task GetOrchestrationRuntimeState()
        {
            var instanceId = Prompt("InstanceId?");
            var state = await serviceClient.GetOrchestrationRuntimeState(instanceId);
            Console.WriteLine(state);
        }

        private static async Task ListOrchestrations()
        {
            foreach (var instance in await serviceClient.GetRunningOrchestrations())
            {
                Console.WriteLine(instance);
            }
        }
    }
}
