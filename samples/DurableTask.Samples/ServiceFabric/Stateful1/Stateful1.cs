using System;
using System.Collections.Generic;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.AzureServiceFabric;
using DurableTask.Core;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Stateful1.Samples.AverageCalculator;
using Stateful1.Samples.Cron;
using Stateful1.Samples.ErrorHandling;
using Stateful1.Samples.Replat;
using Stateful1.Samples.Signal;

namespace Stateful1
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class Stateful1 : StatefulService
    {
        public Stateful1(StatefulServiceContext context)
            : base(context)
        { }

        /// <summary>
        /// Optional override to create listeners (e.g., HTTP, Service Remoting, WCF, etc.) for this service replica to handle client or user requests.
        /// </summary>
        /// <remarks>
        /// For more information on service communication, see https://aka.ms/servicefabricservicecommunication
        /// </remarks>
        /// <returns>A collection of listeners.</returns>
        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return new ServiceReplicaListener[0];
        }

        /// <summary>
        /// This is the main entry point for your service replica.
        /// This method executes when this replica of your service becomes primary and has write status.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service replica.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            try
            {
                var taskProvider = (new FabricOrchestrationProviderFactory(StateManager)).CreateProvider();

                var taskHubClient = new TaskHubClient(taskProvider.OrchestrationServiceClient);
                var taskHub = new TaskHubWorker(taskProvider.OrchestrationService);

                OrchestrationInstance instance = null;

                var instanceId = Guid.NewGuid().ToString();
                ServiceEventSource.Current.Message($"Start orchestration");
                // instance = await taskHubClient.CreateOrchestrationInstanceAsync(typeof(CronOrchestration), instanceId,
                //     "0 12 **/2 Mon");

                int[] input = {10, 50, 10};
                instance = await taskHubClient.CreateOrchestrationInstanceAsync(typeof(AverageCalculatorOrchestration),
                    instanceId, input);
                
                // instance = await taskHubClient.CreateOrchestrationInstanceAsync(typeof(MigrateOrchestration),
                //     instanceId,
                //     new MigrateOrchestrationData
                //         {SubscriptionId = "03a1cd39-47ac-4a57-9ff5-a2c2a2a76088", IsDisabled = false});


                taskHub.AddTaskOrchestrations(
                    typeof(CronOrchestration),
                    typeof(AverageCalculatorOrchestration),
                    typeof(ErrorHandlingOrchestration),
                    typeof(SignalOrchestration),
                    typeof(MigrateOrchestration));

                taskHub.AddTaskActivities(
                    new CronTask(),
                    new ComputeSumTask(),
                    new GoodTask(),
                    new BadTask(),
                    new CleanupTask());

                taskHub.AddTaskActivitiesFromInterface<IManagementSqlOrchestrationTasks>(
                    new ManagementSqlOrchestrationTasks());
                taskHub.AddTaskActivitiesFromInterface<IMigrationTasks>(new MigrationTasks());

                taskHub.StartAsync().Wait();

                ServiceEventSource.Current.Message("Waiting up to 60 seconds for completion");

                var taskResult =
                    await taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(60),
                        CancellationToken.None);
                ServiceEventSource.Current.Message($"Task done: {taskResult.OrchestrationStatus}");

                await taskHub.StopAsync(true);
            }
            catch (Exception ex)
            {
                ServiceEventSource.Current.Message($"worker exception: {ex}");
            }
        }
    }
}
