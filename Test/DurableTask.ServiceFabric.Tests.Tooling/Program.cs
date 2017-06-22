using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Services.Client;
using Microsoft.ServiceFabric.Services.Remoting.Client;
using TestApplication.Common;

namespace DurableTask.ServiceFabric.Tests.Tooling
{
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
