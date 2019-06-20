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

namespace TestStatefulService
{
    using System;
    using System.Collections.Generic;
    using System.Fabric;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.ServiceFabric;
    using DurableTask.Test.Orchestrations.Perf;
    using Microsoft.ServiceFabric.Services.Communication.Runtime;
    using Microsoft.ServiceFabric.Services.Remoting.Runtime;
    using Microsoft.ServiceFabric.Services.Runtime;
    using TestApplication.Common;
    using TestOrchestrations;

    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class TestStatefulService : StatefulService, IRemoteClient
    {
        readonly FabricOrchestrationProviderFactory fabricProviderFactory;

        FabricOrchestrationProvider fabricProvider;
        TaskHubWorker worker;
        TaskHubClient client;
        ReplicaRole currentRole;

        public TestStatefulService(StatefulServiceContext context) : base(context)
        {
            var settings = new FabricOrchestrationProviderSettings();
            settings.TaskOrchestrationDispatcherSettings.DispatcherCount = 5;
            settings.TaskActivityDispatcherSettings.DispatcherCount = 5;

            this.fabricProviderFactory = new FabricOrchestrationProviderFactory(this.StateManager, settings);
            this.fabricProvider = this.fabricProviderFactory.CreateProvider();
            this.client = new TaskHubClient(fabricProvider.OrchestrationServiceClient);
        }

        /// <summary>
        /// Optional override to create listeners (e.g., HTTP, Service Remoting, WCF, etc.) for this service replica to handle client or user requests.
        /// </summary>
        /// <remarks>
        /// For more information on service communication, see http://aka.ms/servicefabricservicecommunication
        /// </remarks>
        /// <returns>A collection of listeners.</returns>
        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return new[]
            {
                //new ServiceReplicaListener(initParams => new OwinCommunicationListener("TestStatefulService", new Startup(), initParams))
                new ServiceReplicaListener(this.CreateServiceRemotingListener)
            };
        }

        /// <summary>
        /// This is the main entry point for your service replica.
        /// This method executes when this replica of your service becomes primary and has write status.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service replica.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            if (this.fabricProvider == null)
            {
                this.fabricProvider = this.fabricProviderFactory.CreateProvider();
                this.client = new TaskHubClient(this.fabricProvider.OrchestrationServiceClient);
            }

            this.worker = new TaskHubWorker(this.fabricProvider.OrchestrationService);

            await this.worker
                .AddTaskOrchestrations(KnownOrchestrationInstances.Values.Select(instance => new DefaultObjectCreator<TaskOrchestration>(instance)).ToArray())
                .AddTaskOrchestrations(KnownOrchestrationTypeNames.Values.ToArray())
                .AddTaskActivities(KnownActivities)
                .StartAsync();

            this.worker.TaskActivityDispatcher.IncludeDetails = true;
        }

        protected override async Task OnChangeRoleAsync(ReplicaRole newRole, CancellationToken cancellationToken)
        {
            ServiceEventSource.Current.ServiceRequestStart($"Fabric On Change Role Async, current role = {this.currentRole}, new role = {newRole}");
            if (newRole != ReplicaRole.Primary && this.currentRole == ReplicaRole.Primary)
            {
                await ShutdownAsync();
            }
            this.currentRole = newRole;
            ServiceEventSource.Current.ServiceRequestStop($"Fabric On Change Role Async, current role = {this.currentRole}");
        }

        protected override async Task OnCloseAsync(CancellationToken cancellationToken)
        {
            ServiceEventSource.Current.ServiceMessage(this, "OnCloseAsync - will shutdown primary if not already done");
            await ShutdownAsync();
        }

        public async Task<OrchestrationState> RunOrchestrationAsync(string orchestrationTypeName, object input, TimeSpan waitTimeout)
        {
            OrchestrationInstance instance = await StartOrchestrationAsync(orchestrationTypeName, input);
            return await client.WaitForOrchestrationAsync(instance, waitTimeout);
        }

        public async Task<OrchestrationInstance> StartOrchestrationAsync(string orchestrationTypeName, object input)
        {
            if (orchestrationTypeName == typeof(ExecutionCountingOrchestration).Name)
            {
                // See ExecutionCountingActivity notes on why we do this.
                ExecutionCountingActivity.Counter = 0;
            }

            var instance = await client.CreateOrchestrationInstanceAsync(GetOrchestrationType(orchestrationTypeName), input);
            return instance;
        }

        public async Task<OrchestrationState> RunDriverOrchestrationAsync(DriverOrchestrationData input, TimeSpan waitTimeout)
        {
            return await this.RunOrchestrationAsync(typeof(DriverOrchestration).Name, input, waitTimeout);
        }

        public Task<OrchestrationInstance> StartTestOrchestrationAsync(TestOrchestrationData input)
        {
            return client.CreateOrchestrationInstanceAsync(typeof(TestOrchestration), input);
        }

        public Task<OrchestrationState> GetOrchestrationState(OrchestrationInstance instance)
        {
            if (instance == null)
            {
                throw new ArgumentNullException(nameof(instance));
            }

            try
            {
                return client.GetOrchestrationStateAsync(instance);
            }
            catch(Exception ex)
            {
                ServiceEventSource.Current.ServiceRequestFailed($"{nameof(GetOrchestrationState)}", ex.ToString());
                throw;
            }
        }

        public async Task<OrchestrationState> WaitForOrchestration(OrchestrationInstance instance, TimeSpan waitTimeout)
        {
            if (instance == null)
            {
                throw new ArgumentNullException(nameof(instance));
            }

            try
            {
                return await client.WaitForOrchestrationAsync(instance, waitTimeout);
            }
            catch(Exception ex)
            {
                ServiceEventSource.Current.ServiceRequestFailed($"{nameof(WaitForOrchestration)}", ex.ToString());
                throw;
            }
        }

        public Task PurgeOrchestrationHistoryEventsAsync()
        {
            return this.client.PurgeOrchestrationInstanceHistoryAsync(DateTime.UtcNow, OrchestrationStateTimeRangeFilterType.OrchestrationCompletedTimeFilter);
        }

        async Task ShutdownAsync()
        {
            try
            {
                if (this.worker != null)
                {
                    ServiceEventSource.Current.ServiceMessage(this, "Stopping Taskhub Worker");
                    await this.worker.StopAsync(isForced: true);
                    this.worker.Dispose();
                    this.fabricProvider.Dispose();
                    this.fabricProvider = null;
                    this.worker = null;
                    ServiceEventSource.Current.ServiceMessage(this, "Stopped Taskhub Worker");
                }
            }
            catch (Exception e)
            {
                ServiceEventSource.Current.ServiceRequestFailed("Exception when Stopping Worker On Primary Stop", e.ToString());
                throw;
            }
        }

        Type GetOrchestrationType(string typeName)
        {
            if (KnownOrchestrationInstances.ContainsKey(typeName))
            {
                return KnownOrchestrationInstances[typeName].GetType();
            }

            if (!KnownOrchestrationTypeNames.ContainsKey(typeName))
            {
                throw new Exception($"Unknown Orchestration Type Name : {typeName}");
            }

            return KnownOrchestrationTypeNames[typeName];
        }

        public Task<OrchestrationInstance> StartTestOrchestrationWithInstanceIdAsync(string instanceId, TestOrchestrationData input)
        {
            return client.CreateOrchestrationInstanceAsync(typeof(TestOrchestration), instanceId, input);
        }

        public async Task<OrchestrationState> GetOrchestrationStateWithInstanceId(string instanceId)
        {
            var allStates = await this.client.GetOrchestrationStateAsync(instanceId, allExecutions: false);
            return allStates.FirstOrDefault();
        }

        public Task<IEnumerable<OrchestrationInstance>> GetRunningOrchestrations()
        {
            return this.fabricProvider.FabricProviderClient.GetRunningOrchestrations();
        }

        public Task<string> GetOrchestrationRuntimeState(string instanceId)
        {
            return this.fabricProvider.FabricProviderClient.GetOrchestrationRuntimeState(instanceId);
        }

        public Task TerminateOrchestration(string instanceId, string reason)
        {
            var instance = new OrchestrationInstance() { InstanceId = instanceId};
            return client.TerminateInstanceAsync(instance, reason);
        }

        static Dictionary<string, TaskOrchestration> KnownOrchestrationInstances = new Dictionary<string, TaskOrchestration>
        {
            { typeof(OrchestrationRunningIntoRetry).Name, new OrchestrationRunningIntoRetry() },
        };

        static Dictionary<string, Type> KnownOrchestrationTypeNames = new Dictionary<string, Type>
        {
            { typeof(SimpleOrchestrationWithTasks).Name, typeof(SimpleOrchestrationWithTasks) },
            { typeof(SimpleOrchestrationWithTimer).Name, typeof(SimpleOrchestrationWithTimer) },
            { typeof(GenerationBasicOrchestration).Name, typeof(GenerationBasicOrchestration) },
            { typeof(SimpleOrchestrationWithSubOrchestration).Name, typeof(SimpleOrchestrationWithSubOrchestration) },
            { typeof(DriverOrchestration).Name, typeof(DriverOrchestration) },
            { typeof(TestOrchestration).Name, typeof(TestOrchestration) },
            { typeof(ExecutionCountingOrchestration).Name, typeof(ExecutionCountingOrchestration) },
        };

        static Type[] KnownActivities = 
        {
            typeof(GetUserTask),
            typeof(GreetUserTask),
            typeof(GenerationBasicTask),
            typeof(RandomTimeWaitingTask),
            typeof(ExceptionThrowingTask),
            typeof(ExecutionCountingActivity),
        };
    }
}
