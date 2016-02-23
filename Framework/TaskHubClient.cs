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

namespace DurableTask
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    using Common;
    using History;
    using Microsoft.ServiceBus;
    using Microsoft.ServiceBus.Messaging;
    using Microsoft.WindowsAzure.Storage.Table;
    using Newtonsoft.Json;
    using Serializing;
    using Tracing;
    using Tracking;

    /// <summary>
    ///     Client used to manage and query orchestration instances
    /// </summary>
    public sealed class TaskHubClient
    {
        readonly string connectionString;
        readonly DataConverter defaultConverter;
        readonly string hubName;
        readonly MessagingFactory messagingFactory;
        readonly string orchestratorEntityName;

        readonly TaskHubClientSettings settings;
        readonly TableClient tableClient;
        readonly string tableStoreConnectionString;
        readonly string workerEntityName;

        /// <summary>
        ///     Create a new TaskHubClient with the given name, service bus connection string and default settings.
        /// </summary>
        /// <param name="hubName">Name of the Task Hub</param>
        /// <param name="connectionString">Service Bus connection string</param>
        public TaskHubClient(string hubName, string connectionString)
            : this(hubName, connectionString, null, new TaskHubClientSettings())
        {
        }

        /// <summary>
        ///     Create a new TaskHubClient with given name and service bus connection string with specified settings.
        /// </summary>
        /// <param name="hubName">Name of the Task Hub</param>
        /// <param name="connectionString">Service Bus connection string</param>
        /// <param name="settings">Client settings</param>
        public TaskHubClient(string hubName, string connectionString, TaskHubClientSettings settings)
            : this(hubName, connectionString, null, settings)
        {
        }

        /// <summary>
        ///     Create a new TaskHubClient with given name, Service Bus and Azure Storage connection strings with default settings.
        /// </summary>
        /// <param name="hubName">Name of the Task Hub</param>
        /// <param name="connectionString">Service Bus connection string</param>
        /// <param name="tableStoreConnectionString">Azure Storage connection string</param>
        public TaskHubClient(string hubName, string connectionString, string tableStoreConnectionString)
            : this(hubName, connectionString, tableStoreConnectionString, new TaskHubClientSettings())
        {
        }


        /// <summary>
        ///     Create a new TaskHubClient with given name, Service Bus and Azure Storage connection strings  with specified
        ///     settings.
        /// </summary>
        /// <param name="hubName">Name of the Task Hub</param>
        /// <param name="connectionString">Service Bus connection string</param>
        /// <param name="tableStoreConnectionString">Azure Storage connection string</param>
        /// <param name="settings">Client settings</param>
        public TaskHubClient(string hubName, string connectionString, string tableStoreConnectionString,
            TaskHubClientSettings settings)
        {
            this.hubName = hubName;
            this.connectionString = connectionString;
            messagingFactory = ServiceBusUtils.CreateMessagingFactory(connectionString);
            workerEntityName = string.Format(FrameworkConstants.WorkerEndpointFormat, this.hubName);
            orchestratorEntityName = string.Format(FrameworkConstants.OrchestratorEndpointFormat, this.hubName);
            defaultConverter = new JsonDataConverter();
            this.settings = settings;

            this.tableStoreConnectionString = tableStoreConnectionString;
            if (!string.IsNullOrEmpty(this.tableStoreConnectionString))
            {
                tableClient = new TableClient(this.hubName, this.tableStoreConnectionString);
            }
        }

        /// <summary>
        ///     Create a new orchestration of the specified type with an automatically generated instance id
        /// </summary>
        /// <param name="orchestrationType">Type that derives from TaskOrchestration</param>
        /// <param name="input">Input parameter to the specified TaskOrchestration</param>
        /// <returns>OrchestrationInstance that represents the orchestration that was created</returns>
        public OrchestrationInstance CreateOrchestrationInstance(Type orchestrationType, object input)
        {
            return Utils.AsyncExceptionWrapper(() => CreateOrchestrationInstanceAsync(orchestrationType, input).Result);
        }

        /// <summary>
        ///     Create a new orchestration of the specified type with an automatically generated instance id
        /// </summary>
        /// <param name="orchestrationType">Type that derives from TaskOrchestration</param>
        /// <param name="input">Input parameter to the specified TaskOrchestration</param>
        /// <returns>OrchestrationInstance that represents the orchestration that was created</returns>
        public Task<OrchestrationInstance> CreateOrchestrationInstanceAsync(Type orchestrationType, object input)
        {
            return CreateOrchestrationInstanceAsync(NameVersionHelper.GetDefaultName(orchestrationType),
                NameVersionHelper.GetDefaultVersion(orchestrationType), input);
        }

        /// <summary>
        ///     Create a new orchestration of the specified type with the specified instance id
        /// </summary>
        /// <param name="orchestrationType">Type that derives from TaskOrchestration</param>
        /// <param name="instanceId">Instance id for the orchestration to be created, must be unique across the Task Hub</param>
        /// <param name="input">Input parameter to the specified TaskOrchestration</param>
        /// <returns>OrchestrationInstance that represents the orchestration that was created</returns>
        public OrchestrationInstance CreateOrchestrationInstance(Type orchestrationType, string instanceId, object input)
        {
            return Utils.AsyncExceptionWrapper(() =>
                CreateOrchestrationInstanceAsync(orchestrationType, instanceId, input).Result);
        }

        /// <summary>
        ///     Create a new orchestration of the specified type with the specified instance id
        /// </summary>
        /// <param name="orchestrationType">Type that derives from TaskOrchestration</param>
        /// <param name="instanceId">Instance id for the orchestration to be created, must be unique across the Task Hub</param>
        /// <param name="input">Input parameter to the specified TaskOrchestration</param>
        /// <returns>OrchestrationInstance that represents the orchestration that was created</returns>
        public Task<OrchestrationInstance> CreateOrchestrationInstanceAsync(Type orchestrationType, string instanceId,
            object input)
        {
            return CreateOrchestrationInstanceAsync(NameVersionHelper.GetDefaultName(orchestrationType),
                NameVersionHelper.GetDefaultVersion(orchestrationType), instanceId, input);
        }

        /// <summary>
        ///     Create a new orchestration of the specified name and version
        /// </summary>
        /// <param name="name">Name of the TaskOrchestration</param>
        /// <param name="version">Version of the TaskOrchestration</param>
        /// <param name="input">Input parameter to the specified TaskOrchestration</param>
        /// <returns>OrchestrationInstance that represents the orchestration that was created</returns>
        public OrchestrationInstance CreateOrchestrationInstance(string name, string version, object input)
        {
            return Utils.AsyncExceptionWrapper(() => CreateOrchestrationInstanceAsync(name, version, input).Result);
        }

        /// <summary>
        ///     Create a new orchestration of the specified name and version
        /// </summary>
        /// <param name="name">Name of the TaskOrchestration</param>
        /// <param name="version">Version of the TaskOrchestration</param>
        /// <param name="input">Input parameter to the specified TaskOrchestration</param>
        /// <returns>OrchestrationInstance that represents the orchestration that was created</returns>
        public Task<OrchestrationInstance> CreateOrchestrationInstanceAsync(string name, string version, object input)
        {
            string instanceId = Guid.NewGuid().ToString("N");
            return CreateOrchestrationInstanceAsync(name, version, instanceId, input);
        }

        /// <summary>
        ///     Create a new orchestration of the specified name and version
        /// </summary>
        /// <param name="name">Name of the orchestration as specified by the ObjectCreator</param>
        /// <param name="version">Name of the orchestration as specified by the ObjectCreator</param>
        /// <param name="instanceId">Instance id for the orchestration to be created, must be unique across the Task Hub</param>
        /// <param name="input">Input parameter to the specified TaskOrchestration</param>
        /// <returns>OrchestrationInstance that represents the orchestration that was created</returns>
        public OrchestrationInstance CreateOrchestrationInstance(string name, string version, string instanceId,
            object input)
        {
            return
                Utils.AsyncExceptionWrapper(
                    () => CreateOrchestrationInstanceAsync(name, version, instanceId, input).Result);
        }

        /// <summary>
        ///     Create a new orchestration of the specified name and version
        /// </summary>
        /// <param name="name">Name of the orchestration as specified by the ObjectCreator</param>
        /// <param name="version">Name of the orchestration as specified by the ObjectCreator</param>
        /// <param name="instanceId">Instance id for the orchestration to be created, must be unique across the Task Hub</param>
        /// <param name="input">Input parameter to the specified TaskOrchestration</param>
        /// <returns>OrchestrationInstance that represents the orchestration that was created</returns>
        public Task<OrchestrationInstance> CreateOrchestrationInstanceAsync(string name, string version,
            string instanceId, object input)
        {
            return CreateOrchestrationInstanceAsync(name, version, instanceId, input, null);
        }

        /// <summary>
        ///     Create a new orchestration of the specified name and version
        /// </summary>
        /// <param name="name">Name of the orchestration as specified by the ObjectCreator</param>
        /// <param name="version">Name of the orchestration as specified by the ObjectCreator</param>
        /// <param name="instanceId">Instance id for the orchestration to be created, must be unique across the Task Hub</param>
        /// <param name="input">Input parameter to the specified TaskOrchestration</param>
        /// <param name="tags">Dictionary of key/value tags associated with this instance</param>
        /// <returns>OrchestrationInstance that represents the orchestration that was created</returns>
        public OrchestrationInstance CreateOrchestrationInstance(string name, string version, string instanceId,
            object input, IDictionary<string, string> tags)
        {
            return
                Utils.AsyncExceptionWrapper(
                    () => CreateOrchestrationInstanceAsync(name, version, instanceId, input, tags).Result);
        }

        /// <summary>
        ///     Create a new orchestration of the specified name and version
        /// </summary>
        /// <param name="name">Name of the orchestration as specified by the ObjectCreator</param>
        /// <param name="version">Name of the orchestration as specified by the ObjectCreator</param>
        /// <param name="instanceId">Instance id for the orchestration to be created, must be unique across the Task Hub</param>
        /// <param name="input">Input parameter to the specified TaskOrchestration</param>
        /// <param name="tags">Dictionary of key/value tags associated with this instance</param>
        /// <returns>OrchestrationInstance that represents the orchestration that was created</returns>
        public async Task<OrchestrationInstance> CreateOrchestrationInstanceAsync(string name, string version,
            string instanceId,
            object input, IDictionary<string, string> tags)
        {
            if (string.IsNullOrWhiteSpace(instanceId))
            {
                instanceId = Guid.NewGuid().ToString("N");
            }

            var orchestrationInstance = new OrchestrationInstance
            {
                InstanceId = instanceId,
                ExecutionId = Guid.NewGuid().ToString("N"),
            };

            string serializedInput = defaultConverter.Serialize(input);
            string serializedtags = tags != null ? defaultConverter.Serialize(tags) : null;

            var startedEvent = new ExecutionStartedEvent(-1, serializedInput)
            {
                Tags = serializedtags,
                Name = name,
                Version = version,
                OrchestrationInstance = orchestrationInstance
            };

            var taskMessage = new TaskMessage
            {
                OrchestrationInstance = orchestrationInstance,
                Event = startedEvent
            };

            BrokeredMessage brokeredMessage = ServiceBusUtils.GetBrokeredMessageFromObject(taskMessage,
                settings.MessageCompressionSettings);
            brokeredMessage.SessionId = instanceId;

            MessageSender sender =
                await messagingFactory.CreateMessageSenderAsync(orchestratorEntityName).ConfigureAwait(false);
            await sender.SendAsync(brokeredMessage).ConfigureAwait(false);
            await sender.CloseAsync().ConfigureAwait(false);

            return orchestrationInstance;
        }

        /// <summary>
        ///     Raises an event in the specified orchestration instance, which eventually causes the OnEvent() method in the
        ///     orchestration to fire.
        /// </summary>
        /// <param name="orchestrationInstance">Instance in which to raise the event</param>
        /// <param name="eventName">Name of the event</param>
        /// <param name="eventData">Data for the event</param>
        public void RaiseEvent(OrchestrationInstance orchestrationInstance, string eventName, object eventData)
        {
            Utils.AsyncExceptionWrapper(() => RaiseEventAsync(orchestrationInstance, eventName, eventData).Wait());
        }

        /// <summary>
        ///     Raises an event in the specified orchestration instance, which eventually causes the OnEvent() method in the
        ///     orchestration to fire.
        /// </summary>
        /// <param name="orchestrationInstance">Instance in which to raise the event</param>
        /// <param name="eventName">Name of the event</param>
        /// <param name="eventData">Data for the event</param>
        public async Task RaiseEventAsync(OrchestrationInstance orchestrationInstance, string eventName,
            object eventData)
        {
            if (orchestrationInstance == null || string.IsNullOrWhiteSpace(orchestrationInstance.InstanceId))
            {
                throw new ArgumentException("orchestrationInstance");
            }

            string serializedInput = defaultConverter.Serialize(eventData);
            var taskMessage = new TaskMessage
            {
                OrchestrationInstance = orchestrationInstance,
                Event = new EventRaisedEvent(-1, serializedInput) {Name = eventName}
            };

            BrokeredMessage brokeredMessage = ServiceBusUtils.GetBrokeredMessageFromObject(taskMessage,
                settings.MessageCompressionSettings);
            brokeredMessage.SessionId = orchestrationInstance.InstanceId;

            MessageSender sender =
                await messagingFactory.CreateMessageSenderAsync(orchestratorEntityName).ConfigureAwait(false);
            await sender.SendAsync(brokeredMessage).ConfigureAwait(false);
            await sender.CloseAsync().ConfigureAwait(false);
        }

        /// <summary>
        ///     Forcefully terminate the specified orchestration instance
        /// </summary>
        /// <param name="orchestrationInstance">Instance to terminate</param>
        public void TerminateInstance(OrchestrationInstance orchestrationInstance)
        {
            TerminateInstance(orchestrationInstance, string.Empty);
        }

        /// <summary>
        ///     Forcefully terminate the specified orchestration instance
        /// </summary>
        /// <param name="orchestrationInstance">Instance to terminate</param>
        public Task TerminateInstanceAsync(OrchestrationInstance orchestrationInstance)
        {
            return TerminateInstanceAsync(orchestrationInstance, string.Empty);
        }

        /// <summary>
        ///     Forcefully terminate the specified orchestration instance with a reason
        /// </summary>
        /// <param name="orchestrationInstance">Instance to terminate</param>
        /// <param name="reason">Reason for terminating the instance</param>
        public void TerminateInstance(OrchestrationInstance orchestrationInstance, string reason)
        {
            Utils.AsyncExceptionWrapper(() => TerminateInstanceAsync(orchestrationInstance, reason).Wait());
        }

        /// <summary>
        ///     Forcefully terminate the specified orchestration instance with a reason
        /// </summary>
        /// <param name="orchestrationInstance">Instance to terminate</param>
        /// <param name="reason">Reason for terminating the instance</param>
        public async Task TerminateInstanceAsync(OrchestrationInstance orchestrationInstance, string reason)
        {
            if (orchestrationInstance == null || string.IsNullOrWhiteSpace(orchestrationInstance.InstanceId))
            {
                throw new ArgumentException("orchestrationInstance");
            }

            string instanceId = orchestrationInstance.InstanceId;

            var taskMessage = new TaskMessage
            {
                OrchestrationInstance = orchestrationInstance,
                Event = new ExecutionTerminatedEvent(-1, reason)
            };

            BrokeredMessage brokeredMessage = ServiceBusUtils.GetBrokeredMessageFromObject(taskMessage,
                settings.MessageCompressionSettings);
            brokeredMessage.SessionId = instanceId;

            MessageSender sender =
                await messagingFactory.CreateMessageSenderAsync(orchestratorEntityName).ConfigureAwait(false);
            await sender.SendAsync(brokeredMessage).ConfigureAwait(false);
            await sender.CloseAsync().ConfigureAwait(false);
        }

        // Instance query methods
        // Orchestration states

        /// <summary>
        ///     Get a list of orchestration states from the instance storage table.
        ///     Throws if an Azure Storage account was not specified in the constructor.
        /// </summary>
        /// <returns>
        ///     List of OrchestrationState objects that represents the list of
        ///     orchestrations in the instance store
        /// </returns>
        public IList<OrchestrationState> GetOrchestrationState()
        {
            ThrowIfInstanceStoreNotConfigured();

            IEnumerable<OrchestrationStateEntity> result = Utils.AsyncExceptionWrapper(() =>
                tableClient.QueryOrchestrationStatesAsync(new OrchestrationStateQuery()).Result);

            return new List<OrchestrationState>(result.Select(stateEntity => stateEntity.State));
        }

        /// <summary>
        ///     Get a list of orchestration states from the instance storage table.
        ///     Throws if an Azure Storage account was not specified in the constructor.
        /// </summary>
        /// <returns>
        ///     List of OrchestrationState objects that represents the list of
        ///     orchestrations in the instance store
        /// </returns>
        public async Task<IList<OrchestrationState>> GetOrchestrationStateAsync()
        {
            ThrowIfInstanceStoreNotConfigured();
            IEnumerable<OrchestrationStateEntity> result =
                await tableClient.QueryOrchestrationStatesAsync(new OrchestrationStateQuery()).ConfigureAwait(false);
            return new List<OrchestrationState>(result.Select(stateEntity => stateEntity.State));
        }

        /// <summary>
        ///     Get a list of orchestration states from the instance storage table for the
        ///     most current execution (generation) of the specified instance.
        ///     Throws if an Azure Storage account was not specified in the constructor.
        /// </summary>
        /// <param name="instanceId">Instance id</param>
        /// <returns>The OrchestrationState of the specified instanceId or null if not found</returns>
        public OrchestrationState GetOrchestrationState(string instanceId)
        {
            return GetOrchestrationState(instanceId, false).FirstOrDefault();
        }

        /// <summary>
        ///     Get a list of orchestration states from the instance storage table for the
        ///     most current execution (generation) of the specified instance.
        ///     Throws if an Azure Storage account was not specified in the constructor.
        /// </summary>
        /// <param name="instanceId">Instance id</param>
        /// <returns>The OrchestrationState of the specified instanceId or null if not found</returns>
        public async Task<OrchestrationState> GetOrchestrationStateAsync(string instanceId)
        {
            return (await GetOrchestrationStateAsync(instanceId, false).ConfigureAwait(false)).FirstOrDefault();
        }

        /// <summary>
        ///     Get a list of orchestration states from the instance storage table which match the specified
        ///     orchestration state query.
        /// </summary>
        /// <param name="stateQuery">Orchestration state query to execute</param>
        /// <returns></returns>
        public IEnumerable<OrchestrationState> QueryOrchestrationStates(OrchestrationStateQuery stateQuery)
        {
            return Utils.AsyncExceptionWrapper(() => QueryOrchestrationStatesAsync(stateQuery).Result);
        }

        /// <summary>
        ///     Get a list of orchestration states from the instance storage table which match the specified
        ///     orchestration state query.
        /// </summary>
        /// <param name="stateQuery">Orchestration state query to execute</param>
        /// <returns></returns>
        public async Task<IEnumerable<OrchestrationState>> QueryOrchestrationStatesAsync(
            OrchestrationStateQuery stateQuery)
        {
            ThrowIfInstanceStoreNotConfigured();
            IEnumerable<OrchestrationStateEntity> result =
                await tableClient.QueryOrchestrationStatesAsync(stateQuery).ConfigureAwait(false);
            return new List<OrchestrationState>(result.Select(stateEntity => stateEntity.State));
        }

        /// <summary>
        ///     Get a segmented list of orchestration states from the instance storage table which match the specified
        ///     orchestration state query. Segment size is controlled by the service.
        /// </summary>
        /// <param name="stateQuery">Orchestration state query to execute</param>
        /// <param name="continuationToken">The token returned from the last query execution. Can be null for the first time.</param>
        /// <returns></returns>
        public OrchestrationStateQuerySegment QueryOrchestrationStatesSegmented(
            OrchestrationStateQuery stateQuery, string continuationToken)
        {
            return
                Utils.AsyncExceptionWrapper(
                    () => QueryOrchestrationStatesSegmentedAsync(stateQuery, continuationToken, -1).Result);
        }

        /// <summary>
        ///     Get a segmented list of orchestration states from the instance storage table which match the specified
        ///     orchestration state query. Segment size is controlled by the service.
        /// </summary>
        /// <param name="stateQuery">Orchestration state query to execute</param>
        /// <param name="continuationToken">The token returned from the last query execution. Can be null for the first time.</param>
        /// <returns></returns>
        public Task<OrchestrationStateQuerySegment> QueryOrchestrationStatesSegmentedAsync(
            OrchestrationStateQuery stateQuery, string continuationToken)
        {
            return QueryOrchestrationStatesSegmentedAsync(stateQuery, continuationToken, -1);
        }

        /// <summary>
        ///     Get a segmented list of orchestration states from the instance storage table which match the specified
        ///     orchestration state query.
        /// </summary>
        /// <param name="stateQuery">Orchestration state query to execute</param>
        /// <param name="continuationToken">The token returned from the last query execution. Can be null for the first time.</param>
        /// <param name="count">Count of elements to return. Service will decide how many to return if set to -1.</param>
        /// <returns></returns>
        public OrchestrationStateQuerySegment QueryOrchestrationStatesSegmented(OrchestrationStateQuery stateQuery,
            string continuationToken, int count)
        {
            return
                Utils.AsyncExceptionWrapper(
                    () => QueryOrchestrationStatesSegmentedAsync(stateQuery, continuationToken, count).Result);
        }

        /// <summary>
        ///     Get a segmented list of orchestration states from the instance storage table which match the specified
        ///     orchestration state query.
        /// </summary>
        /// <param name="stateQuery">Orchestration state query to execute</param>
        /// <param name="continuationToken">The token returned from the last query execution. Can be null for the first time.</param>
        /// <param name="count">Count of elements to return. Service will decide how many to return if set to -1.</param>
        /// <returns></returns>
        public async Task<OrchestrationStateQuerySegment> QueryOrchestrationStatesSegmentedAsync(
            OrchestrationStateQuery stateQuery, string continuationToken, int count)
        {
            ThrowIfInstanceStoreNotConfigured();

            TableContinuationToken tokenObj = null;

            if (continuationToken != null)
            {
                tokenObj = DeserializeTableContinuationToken(continuationToken);
            }

            TableQuerySegment<OrchestrationStateEntity> results =
                await
                    tableClient.QueryOrchestrationStatesSegmentedAsync(stateQuery, tokenObj, count)
                        .ConfigureAwait(false);

            return new OrchestrationStateQuerySegment
            {
                Results = results.Results.Select(s => s.State),
                ContinuationToken = results.ContinuationToken == null
                    ? null
                    : SerializeTableContinuationToken(results.ContinuationToken)
            };
        }

        /// <summary>
        ///     Get a list of orchestration states from the instance storage table for either the most current
        ///     or all executions (generations) of the specified instance.
        ///     Throws if an Azure Storage account was not specified in the constructor.
        /// </summary>
        /// <param name="instanceId">Instance id</param>
        /// <param name="allExecutions">
        ///     True if method should fetch all executions of the instance,
        ///     false if the method should only fetch the most recent execution
        /// </param>
        /// <returns>
        ///     List of OrchestrationState objects that represents the list of
        ///     orchestrations in the instance store
        /// </returns>
        public IList<OrchestrationState> GetOrchestrationState(string instanceId, bool allExecutions)
        {
            return Utils.AsyncExceptionWrapper(() => GetOrchestrationStateAsync(instanceId, allExecutions).Result);
        }

        /// <summary>
        ///     Get a list of orchestration states from the instance storage table for either the most current
        ///     or all executions (generations) of the specified instance.
        ///     Throws if an Azure Storage account was not specified in the constructor.
        /// </summary>
        /// <param name="instanceId">Instance id</param>
        /// <param name="allExecutions">
        ///     True if method should fetch all executions of the instance,
        ///     false if the method should only fetch the most recent execution
        /// </param>
        /// <returns>
        ///     List of OrchestrationState objects that represents the list of
        ///     orchestrations in the instance store
        /// </returns>
        public async Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions)
        {
            ThrowIfInstanceStoreNotConfigured();

            IEnumerable<OrchestrationStateEntity> states =
                await tableClient.QueryOrchestrationStatesAsync(new OrchestrationStateQuery()
                    .AddInstanceFilter(instanceId)).ConfigureAwait(false);

            var returnedStates = new List<OrchestrationState>();

            if (allExecutions)
            {
                returnedStates.AddRange(states.Select(stateEntity => stateEntity.State));
            }
            else
            {
                returnedStates.Add(FindLatestExecution(states));
            }
            return returnedStates;
        }

        /// <summary>
        ///     Get a list of orchestration states from the instance storage table for the
        ///     most current execution (generation) of the specified instance.
        ///     Throws if an Azure Storage account was not specified in the constructor.
        /// </summary>
        /// <param name="instance">Instance</param>
        /// <returns>The OrchestrationState of the specified instanceId or null if not found</returns>
        public OrchestrationState GetOrchestrationState(OrchestrationInstance instance)
        {
            return Utils.AsyncExceptionWrapper(() => GetOrchestrationStateAsync(instance).Result);
        }

        /// <summary>
        ///     Get a list of orchestration states from the instance storage table for the
        ///     most current execution (generation) of the specified instance.
        ///     Throws if an Azure Storage account was not specified in the constructor.
        /// </summary>
        /// <param name="instance">Instance</param>
        /// <returns>The OrchestrationState of the specified instanceId or null if not found</returns>
        public Task<OrchestrationState> GetOrchestrationStateAsync(OrchestrationInstance instance)
        {
            return GetOrchestrationStateAsync(instance.InstanceId, instance.ExecutionId);
        }

        /// <summary>
        ///     Get a list of orchestration states from the instance storage table for the
        ///     specified execution (generation) of the specified instance.
        ///     Throws if an Azure Storage account was not specified in the constructor.
        /// </summary>
        /// <param name="instanceId">Instance id</param>
        /// <param name="executionId">Exectuion id</param>
        /// <returns>The OrchestrationState of the specified instanceId or null if not found</returns>
        public OrchestrationState GetOrchestrationState(string instanceId, string executionId)
        {
            return Utils.AsyncExceptionWrapper(() => GetOrchestrationStateAsync(instanceId, executionId).Result);
        }

        /// <summary>
        ///     Get a list of orchestration states from the instance storage table for the
        ///     specified execution (generation) of the specified instance.
        ///     Throws if an Azure Storage account was not specified in the constructor.
        /// </summary>
        /// <param name="instanceId">Instance id</param>
        /// <param name="executionId">Exectuion id</param>
        /// <returns>The OrchestrationState of the specified instanceId or null if not found</returns>
        public async Task<OrchestrationState> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            if (string.IsNullOrEmpty(instanceId))
            {
                throw new ArgumentException("instanceId");
            }

            if (string.IsNullOrEmpty(executionId))
            {
                throw new ArgumentException("executionId");
            }

            ThrowIfInstanceStoreNotConfigured();
            OrchestrationStateEntity stateEntity = (await tableClient.QueryOrchestrationStatesAsync(
                new OrchestrationStateQuery()
                    .AddInstanceFilter(instanceId, executionId)).ConfigureAwait(false))
                .FirstOrDefault();

            return stateEntity != null ? stateEntity.State : null;
        }

        // Orchestration History

        /// <summary>
        ///     Get a string dump of the execution history of the specified orchestration instance
        ///     specified execution (generation) of the specified instance.
        ///     Throws if an Azure Storage account was not specified in the constructor.
        /// </summary>
        /// <param name="instance">Instance</param>
        /// <returns>String with formatted JSON representing the execution history</returns>
        public string GetOrchestrationHistory(OrchestrationInstance instance)
        {
            return Utils.AsyncExceptionWrapper(() => GetOrchestrationHistoryAsync(instance).Result);
        }

        /// <summary>
        ///     Get a string dump of the execution history of the specified orchestration instance
        ///     specified execution (generation) of the specified instance.
        ///     Throws if an Azure Storage account was not specified in the constructor.
        /// </summary>
        /// <param name="instance">Instance</param>
        /// <returns>String with formatted JSON representing the execution history</returns>
        public async Task<string> GetOrchestrationHistoryAsync(OrchestrationInstance instance)
        {
            if (instance == null || string.IsNullOrEmpty(instance.InstanceId) ||
                string.IsNullOrEmpty(instance.ExecutionId))
            {
                throw new ArgumentNullException("instance");
            }

            ThrowIfInstanceStoreNotConfigured();

            IEnumerable<OrchestrationHistoryEventEntity> eventEntities =
                await
                    tableClient.ReadOrchestrationHistoryEventsAsync(instance.InstanceId, instance.ExecutionId)
                        .ConfigureAwait(false);
            var events = new List<HistoryEvent>(eventEntities
                .OrderBy(ee => ee.SequenceNumber)
                .Select(historyEventEntity => historyEventEntity.HistoryEvent));

            string history = JsonConvert.SerializeObject(
                events, Formatting.Indented, new JsonSerializerSettings
                {
                    TypeNameHandling = TypeNameHandling.Objects
                });
            return history;
        }

        /// <summary>
        ///     Purges orchestration instance state and history for orchestrations older than the specified threshold time.
        /// </summary>
        /// <param name="thresholdDateTimeUtc">Threshold date time in UTC</param>
        /// <param name="timeRangeFilterType">What to compare the threshold date time against</param>
        /// <returns></returns>
        public async Task PurgeOrchestrationInstanceHistoryAsync(DateTime thresholdDateTimeUtc,
            OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            ThrowIfInstanceStoreNotConfigured();

            TableContinuationToken continuationToken = null;

            TraceHelper.Trace(TraceEventType.Information,
                () =>
                    "Purging orchestration instances before: " + thresholdDateTimeUtc + ", Type: " + timeRangeFilterType);

            int purgeCount = 0;
            do
            {
                TableQuerySegment<OrchestrationStateEntity> resultSegment =
                    (await tableClient.QueryOrchestrationStatesSegmentedAsync(
                        new OrchestrationStateQuery()
                            .AddTimeRangeFilter(DateTime.MinValue, thresholdDateTimeUtc, timeRangeFilterType),
                        continuationToken, 100)
                        .ConfigureAwait(false));

                continuationToken = resultSegment.ContinuationToken;

                if (resultSegment.Results != null)
                {
                    await PurgeOrchestrationHistorySegmentAsync(resultSegment).ConfigureAwait(false);
                    purgeCount += resultSegment.Results.Count;
                }
            } while (continuationToken != null);

            TraceHelper.Trace(TraceEventType.Information, () => "Purged " + purgeCount + " orchestration histories");
        }

        async Task PurgeOrchestrationHistorySegmentAsync(
            TableQuerySegment<OrchestrationStateEntity> orchestrationStateEntitySegment)
        {
            var stateEntitiesToDelete = new List<OrchestrationStateEntity>(orchestrationStateEntitySegment.Results);

            var historyEntitiesToDelete = new ConcurrentBag<IEnumerable<OrchestrationHistoryEventEntity>>();
            await Task.WhenAll(orchestrationStateEntitySegment.Results.Select(
                entity => Task.Run(async () =>
                {
                    IEnumerable<OrchestrationHistoryEventEntity> historyEntities =
                        await
                            tableClient.ReadOrchestrationHistoryEventsAsync(
                                entity.State.OrchestrationInstance.InstanceId,
                                entity.State.OrchestrationInstance.ExecutionId).ConfigureAwait(false);

                    historyEntitiesToDelete.Add(historyEntities);
                })));

            List<Task> historyDeleteTasks = historyEntitiesToDelete.Select(
                historyEventList => tableClient.DeleteEntitesAsync(historyEventList)).Cast<Task>().ToList();

            // need to serialize history deletes before the state deletes so we dont leave orphaned history events
            await Task.WhenAll(historyDeleteTasks).ConfigureAwait(false);
            await Task.WhenAll(tableClient.DeleteEntitesAsync(stateEntitiesToDelete)).ConfigureAwait(false);
        }

        static OrchestrationState FindLatestExecution(IEnumerable<OrchestrationStateEntity> stateEntities)
        {
            foreach (OrchestrationStateEntity stateEntity in stateEntities)
            {
                if (stateEntity.State.OrchestrationStatus != OrchestrationStatus.ContinuedAsNew)
                {
                    return stateEntity.State;
                }
            }
            return null;
        }

        void ThrowIfInstanceStoreNotConfigured()
        {
            if (tableClient == null)
            {
                throw new InvalidOperationException("Instance store is not configured");
            }
        }

        // Management operations

        /// <summary>
        ///     Get the count of pending orchestrations in the TaskHub
        /// </summary>
        /// <returns>Count of pending orchestrations</returns>
        public long GetPendingOrchestrationsCount()
        {
            return GetQueueCount(orchestratorEntityName);
        }

        /// <summary>
        ///     Get the count of pending work items (activities) in the TaskHub
        /// </summary>
        /// <returns>Count of pending activities</returns>
        public long GetPendingWorkItemsCount()
        {
            return GetQueueCount(workerEntityName);
        }

        long GetQueueCount(string entityName)
        {
            NamespaceManager namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);
            QueueDescription queueDescription = namespaceManager.GetQueue(entityName);
            if (queueDescription == null)
            {
                throw TraceHelper.TraceException(TraceEventType.Error,
                    new ArgumentException("Queue " + entityName + " does not exist"));
            }
            return queueDescription.MessageCount;
        }

        string SerializeTableContinuationToken(TableContinuationToken continuationToken)
        {
            if (continuationToken == null)
            {
                throw new ArgumentNullException("continuationToken");
            }

            string serializedToken = JsonConvert.SerializeObject(continuationToken,
                new JsonSerializerSettings {TypeNameHandling = TypeNameHandling.None});
            return Convert.ToBase64String(Encoding.Unicode.GetBytes(serializedToken));
        }

        TableContinuationToken DeserializeTableContinuationToken(string serializedContinuationToken)
        {
            if (string.IsNullOrWhiteSpace(serializedContinuationToken))
            {
                throw new ArgumentException("Invalid serializedContinuationToken");
            }

            byte[] tokenBytes = Convert.FromBase64String(serializedContinuationToken);

            return JsonConvert.DeserializeObject<TableContinuationToken>(Encoding.Unicode.GetString(tokenBytes));
        }
    }
}