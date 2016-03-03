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

namespace DurableTask.Test
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Common;
    using DurableTask.Exceptions;
    using DurableTask.Serializing;

    internal class FakeOrchestrationExecutor
    {
        readonly IDictionary<OrchestrationInstance, TaskOrchestration> completedExecutions =
            new Dictionary<OrchestrationInstance, TaskOrchestration>();

        readonly IDictionary<string, TaskOrchestration> currentExecutions = new Dictionary<string, TaskOrchestration>();
        readonly JsonDataConverter dataConverter;

        readonly NameVersionObjectManager<TaskOrchestration> orchestrationObjectManager;
        readonly TaskScheduler scheduler;

        public FakeOrchestrationExecutor(NameVersionObjectManager<TaskOrchestration> orchestrationObjectManager)
        {
            scheduler = new SynchronousTaskScheduler();
            dataConverter = new JsonDataConverter();
            this.orchestrationObjectManager = orchestrationObjectManager;
        }

        public Task<T> ExecuteOrchestration<T>(OrchestrationInstance instance, FakeOrchestrationContext parentContext,
            string name, string version, object input)
        {
            if (instance == null)
            {
                throw new ArgumentNullException("instance");
            }

            return ExecuteOrchestration<T>(instance, parentContext.TaskActivityExecutor, parentContext.Clock, name,
                version, input);
        }

        public async Task<T> ExecuteOrchestration<T>(OrchestrationInstance instance,
            FakeTaskActivityExecutor taskActivityExecutor,
            FakeOrchestrationClock clock, string name, string version, object input)
        {
            string actualVersion = version;

            if (instance == null)
            {
                throw new ArgumentNullException("instance");
            }

            if (string.IsNullOrWhiteSpace(instance.InstanceId))
            {
                instance.InstanceId = Guid.NewGuid().ToString("N");
            }

            if (string.IsNullOrWhiteSpace(instance.ExecutionId))
            {
                instance.ExecutionId = Guid.NewGuid().ToString("N");
            }

            if (currentExecutions.ContainsKey(instance.InstanceId))
            {
                throw new OrchestrationFrameworkException("Orchestration instance with id '" + instance.InstanceId +
                                                          "' already running.");
            }

            T result = default(T);
            FakeOrchestrationContext context;
            do
            {
                context = new FakeOrchestrationContext(scheduler, instance, taskActivityExecutor, this, clock);
                result = await Execute<T>(context, name, actualVersion, input);
                if (context.StartNew)
                {
                    actualVersion = context.NewVersion ?? actualVersion;

                    while (clock.HasPendingTimers || taskActivityExecutor.HasPendingExecutions)
                    {
                        // wait for pending tasks to complete before starting new generation
                        await Task.Delay(10);
                    }

                    input = context.ContinueAsNewInput;
                    instance.ExecutionId = Guid.NewGuid().ToString("N");
                }
            } while (context.StartNew);

            return result;
        }

        public void RaiseEvent(OrchestrationInstance instance, string eventName, object eventData)
        {
            if (instance == null || string.IsNullOrWhiteSpace(instance.InstanceId))
            {
                throw new ArgumentException("instance");
            }

            TaskOrchestration execution = null;
            if (!currentExecutions.TryGetValue(instance.InstanceId, out execution))
            {
                throw new OrchestrationFrameworkException("Unknown orchestration instance.  Id: " + instance.InstanceId);
            }

            string serializedInput = dataConverter.Serialize(eventData);
            execution.RaiseEvent(null, eventName, serializedInput);
        }

        public T GetStatus<T>(OrchestrationInstance instance)
        {
            if (instance == null || string.IsNullOrWhiteSpace(instance.InstanceId))
            {
                throw new ArgumentException("instance");
            }

            TaskOrchestration execution = null;
            if (!(currentExecutions.TryGetValue(instance.InstanceId, out execution)
                  || completedExecutions.TryGetValue(instance, out execution)))
            {
                throw new OrchestrationFrameworkException("Unknown orchestration instance.  Id: " + instance.InstanceId);
            }

            string status = execution.GetStatus();
            return dataConverter.Deserialize<T>(status);
        }

        async Task<T> Execute<T>(FakeOrchestrationContext context, string name, string version, object input)
        {
            OrchestrationInstance instance = context.OrchestrationInstance;
            SynchronizationContext prevCtx = SynchronizationContext.Current;
            try
            {
                TaskOrchestration definition = orchestrationObjectManager.GetObject(name, version);
                if (definition == null)
                {
                    throw new OrchestrationFrameworkException("Orchestration not found");
                }

                string serializedInput = dataConverter.Serialize(input);
                Task<string> serializedResultTask = definition.Execute(context, serializedInput);
                currentExecutions.Add(instance.InstanceId, definition);

                string serializedResult = null;
                try
                {
                    serializedResult = await serializedResultTask;
                }
                catch (OrchestrationFailureException e)
                {
                    Exception cause = Utils.RetrieveCause(e.Details, dataConverter);
                    var subOrchestrationFailedException = new SubOrchestrationFailedException(0, 0, name, version,
                        e.Message, cause);
                    throw subOrchestrationFailedException;
                }
                catch (Exception e)
                {
                    var subOrchestrationFailedException = new SubOrchestrationFailedException(0, 0, name, version,
                        e.Message, e);
                    throw subOrchestrationFailedException;
                }

                return dataConverter.Deserialize<T>(serializedResult);
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(prevCtx);
                TaskOrchestration orchestration = null;
                if (currentExecutions.TryGetValue(instance.InstanceId, out orchestration))
                {
                    currentExecutions.Remove(instance.InstanceId);
                    completedExecutions.Add(instance, orchestration);
                }
            }
        }
    }
}