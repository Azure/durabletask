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
    using DurableTask.Core;
    using DurableTask.Core.Settings;
    using Microsoft.Extensions.Logging;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;

    internal sealed class TestOrchestrationHost : IDisposable
    {
        internal readonly AzureStorageOrchestrationService service;

        readonly AzureStorageOrchestrationServiceSettings settings;
        readonly TaskHubClient client;
        readonly HashSet<Type> addedOrchestrationTypes;
        readonly HashSet<Type> addedActivityTypes;

        // We allow updates to the worker for versioning tests.
        TaskHubWorker worker;

        public TestOrchestrationHost(AzureStorageOrchestrationServiceSettings settings, VersioningSettings versioningSettings = null)
        {
            this.service = new AzureStorageOrchestrationService(settings);
            this.service.CreateAsync().GetAwaiter().GetResult();

            this.settings = settings;
            this.worker = new TaskHubWorker(service, loggerFactory: settings.LoggerFactory, versioningSettings: versioningSettings);
            this.client = new TaskHubClient(service, loggerFactory: settings.LoggerFactory);
            this.addedOrchestrationTypes = new HashSet<Type>();
            this.addedActivityTypes = new HashSet<Type>();
        }

        public string TaskHub => this.settings.TaskHubName;

        public void Dispose()
        {
            this.worker.Dispose();
        }

        public Task StartAsync()
        {
            return this.worker.StartAsync();
        }

        public Task StopAsync()
        {
            return this.worker.StopAsync(isForced: true);
        }

        public async Task UpdateWorkerVersion(VersioningSettings versioningSettings)
        {
            // Stop the current worker and create a new one with the new versioning settings.
            await this.worker.StopAsync();
            this.worker = new TaskHubWorker(this.service, loggerFactory: this.settings.LoggerFactory, versioningSettings: versioningSettings);
            await this.worker.StartAsync();
        }

        public void AddAutoStartOrchestrator(Type type)
        {
            this.worker.AddTaskOrchestrations(new AutoStartOrchestrationCreator(type));
            this.addedOrchestrationTypes.Add(type);
        }

        public async Task<TestOrchestrationClient> StartOrchestrationAsync(
            Type orchestrationType,
            object input,
            string instanceId = null,
            DateTime? startAt = null,
            IDictionary<string, string> tags = null,
            string version = null)
        {
            if (startAt != null && tags != null)
            {
                throw new NotSupportedException("Cannot set both startAt and tags parameters.");
            }

            if (!this.addedOrchestrationTypes.Contains(orchestrationType))
            {
                if (version != null)
                {
                    this.worker.AddTaskOrchestrations(new NameValueObjectCreator<TaskOrchestration>(
                        NameVersionHelper.GetDefaultName(orchestrationType),
                        version,
                        orchestrationType));
                }
                else
                {
                    this.worker.AddTaskOrchestrations(orchestrationType);
                }
                this.addedOrchestrationTypes.Add(orchestrationType);
            }

            // Allow orchestration types to declare which activity types they depend on.
            // CONSIDER: Make this a supported pattern in DTFx?
            KnownTypeAttribute[] knownTypes =
                (KnownTypeAttribute[])orchestrationType.GetCustomAttributes(typeof(KnownTypeAttribute), false);

            foreach (KnownTypeAttribute referencedKnownType in knownTypes)
            {
                bool orch = referencedKnownType.Type.IsSubclassOf(typeof(TaskOrchestration));
                bool activ = referencedKnownType.Type.IsSubclassOf(typeof(TaskActivity));
                if (orch && !this.addedOrchestrationTypes.Contains(referencedKnownType.Type))
                {
                    this.worker.AddTaskOrchestrations(referencedKnownType.Type);
                    this.addedOrchestrationTypes.Add(referencedKnownType.Type);
                }

                else if (activ && !this.addedActivityTypes.Contains(referencedKnownType.Type))
                {
                    this.worker.AddTaskActivities(referencedKnownType.Type);
                    this.addedActivityTypes.Add(referencedKnownType.Type);
                }
            }

            DateTime creationTime = DateTime.UtcNow;
            OrchestrationInstance instance;
            string orchestrationVersion = !string.IsNullOrEmpty(version) ? version : NameVersionHelper.GetDefaultVersion(orchestrationType);
            if (tags != null)
            {
                instance = await this.client.CreateOrchestrationInstanceAsync(
                    NameVersionHelper.GetDefaultName(orchestrationType),
                    orchestrationVersion,
                    instanceId,
                    input,
                    tags);
            }
            else if (startAt.HasValue)
            {
                instance = await this.client.CreateScheduledOrchestrationInstanceAsync(
                    orchestrationType,
                    instanceId,
                    input,
                    startAt.Value);
            }
            else
            {
                instance = await this.client.CreateOrchestrationInstanceAsync(
                    orchestrationType,
                    instanceId,
                    input);
            }

            Trace.TraceInformation($"Started {orchestrationType.Name}, Instance ID = {instance.InstanceId}");
            return new TestOrchestrationClient(this.client, orchestrationType, instance.InstanceId, creationTime);
        }

        public Task<TestInstance<TInput>> StartInlineOrchestration<TOutput, TInput>(
            TInput input,
            string orchestrationName,
            Func<OrchestrationContext, TInput, Task<TOutput>> implementation,
            Action<OrchestrationContext, string, string> onEvent = null,
            params (string name, TaskActivity activity)[] activities) =>
            this.StartInlineOrchestration(input, orchestrationName, null, implementation, onEvent, activities);

        public async Task<TestInstance<TInput>> StartInlineOrchestration<TOutput, TInput>(
            TInput input,
            string orchestrationName,
            string instanceId,
            Func<OrchestrationContext, TInput, Task<TOutput>> implementation,
            Action<OrchestrationContext, string, string> onEvent = null,
            params (string name, TaskActivity activity)[] activities)
        {
            var instances = await this.StartInlineOrchestrations(
                count: 1,
                instanceIdGenerator: _ => instanceId ?? Guid.NewGuid().ToString("N"),
                inputGenerator: _ => input,
                orchestrationName: orchestrationName,
                version: string.Empty,
                implementation,
                onEvent,
                activities);

            return instances.First();
        }

        public async Task<List<TestInstance<TInput>>> StartInlineOrchestrations<TOutput, TInput>(
            int count,
            Func<int, string> instanceIdGenerator,
            Func<int, TInput> inputGenerator,
            string orchestrationName,
            string version,
            Func<OrchestrationContext, TInput, Task<TOutput>> implementation,
            Action<OrchestrationContext, string, string> onEvent = null,
            params (string name, TaskActivity activity)[] activities)
        {
            // Register the inline orchestration - note that this will only work once per test
            this.RegisterInlineOrchestration(orchestrationName, version, implementation, onEvent);

            foreach ((string name, TaskActivity activity) in activities)
            {
                this.worker.AddTaskActivities(new TestObjectCreator<TaskActivity>(name, activity));
            }

            IEnumerable<Task<TestInstance<TInput>>> tasks = Enumerable.Range(0, count).Select(async i =>
            {
                string instanceId = instanceIdGenerator(i);
                TInput input = inputGenerator(i);

                DateTime utcNow = DateTime.UtcNow;
                OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(
                    orchestrationName,
                    version,
                    instanceId,
                    input);

                return new TestInstance<TInput>(this.client, instance, utcNow, input);
            });

            var instances = new List<TestInstance<TInput>>(count);
            foreach (TestInstance<TInput> instance in await Task.WhenAll(tasks))
            {
                // Verify that the CreateOrchestrationInstanceAsync implementation set the InstanceID and ExecutionID fields
                Assert.IsNotNull(instance.InstanceId);
                Assert.IsNotNull(instance.ExecutionId);

                instances.Add(instance);
            }

            return instances;
        }

        public void RegisterInlineOrchestration<TOutput, TInput>(
            string orchestrationName,
            string version,
            Func<OrchestrationContext, TInput, Task<TOutput>> implementation,
            Action<OrchestrationContext, string, string> onEvent = null)
        {
            this.worker.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(
                orchestrationName,
                version,
                MakeOrchestration(implementation, onEvent)));
        }

        public static TaskOrchestration MakeOrchestration<TOutput, TInput>(
            Func<OrchestrationContext, TInput, Task<TOutput>> implementation,
            Action<OrchestrationContext, string, string> onEvent = null)
        {
            return new OrchestrationShim<TOutput, TInput>(implementation, onEvent);
        }

        // This is just a wrapper around the constructor for convenience. It allows us to write 
        // less code because generic arguments for methods can be implied, unlike constructors.
        public static TaskActivity MakeActivity<TInput, TOutput>(
            Func<TaskContext, TInput, TOutput> implementation)
        {
            return new ActivityShim<TInput, TOutput>(implementation);
        }

        static string GetFriendlyTypeName(Type type)
        {
            string friendlyName = type.Name;
            if (type.IsGenericType)
            {
                int iBacktick = friendlyName.IndexOf('`');
                if (iBacktick > 0)
                {
                    friendlyName = friendlyName.Remove(iBacktick);
                }

                friendlyName += "<";
                Type[] typeParameters = type.GetGenericArguments();
                for (int i = 0; i < typeParameters.Length; ++i)
                {
                    string typeParamName = GetFriendlyTypeName(typeParameters[i]);
                    friendlyName += (i == 0 ? typeParamName : "," + typeParamName);
                }

                friendlyName += ">";
            }

            return friendlyName;
        }

        public async Task<IList<OrchestrationState>> GetAllOrchestrationInstancesAsync()
        {
            // This API currently only exists in the service object and is not yet exposed on the TaskHubClient
            AzureStorageOrchestrationService service = (AzureStorageOrchestrationService)this.client.ServiceClient;
            IList<OrchestrationState> instances = await service.GetOrchestrationStateAsync();
            Trace.TraceInformation($"Found {instances.Count} in the task hub instance store.");
            return instances;
        }

        class ActivityShim<TInput, TOutput> : TaskActivity<TInput, TOutput>
        {
            public ActivityShim(Func<TaskContext, TInput, TOutput> implementation)
            {
                this.Implementation = implementation;
            }

            public Func<TaskContext, TInput, TOutput> Implementation { get; }

            protected override TOutput Execute(TaskContext context, TInput input)
            {
                return this.Implementation(context, input);
            }
        }

        class OrchestrationShim<TOutput, TInput> : TaskOrchestration<TOutput, TInput>
        {
            public OrchestrationShim(
                Func<OrchestrationContext, TInput, Task<TOutput>> implementation,
                Action<OrchestrationContext, string, string> onEvent = null)
            {
                this.Implementation = implementation;
                this.OnEventRaised = onEvent;
            }

            public Func<OrchestrationContext, TInput, Task<TOutput>> Implementation { get; set; }

            public Action<OrchestrationContext, string, string> OnEventRaised { get; set; }

            public override Task<TOutput> RunTask(OrchestrationContext context, TInput input)
                => this.Implementation(context, input);

            public override void RaiseEvent(OrchestrationContext context, string name, string input)
                => this.OnEventRaised(context, name, input);
        }

        class TestObjectCreator<T> : ObjectCreator<T>
        {
            readonly T obj;

            public TestObjectCreator(string name, T obj)
                : this(name, string.Empty, obj)
            {
            }

            public TestObjectCreator(string name, string version, T obj)
            {
                this.Name = name;
                this.Version = version;
                this.obj = obj;
            }

            public override T Create() => this.obj;
        }
    }
}
