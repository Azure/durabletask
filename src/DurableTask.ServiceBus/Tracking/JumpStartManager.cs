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

namespace DurableTask.ServiceBus.Tracking
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.History;
    using DurableTask.Core.Tracing;
    using DurableTask.Core.Tracking;

    internal class JumpStartManager
    {
        readonly ServiceBusOrchestrationService service;
        readonly TimeSpan interval;
        readonly TimeSpan intervalOnTimeout = TimeSpan.MinValue;
        readonly TimeSpan ignoreWindow;
        volatile int isStarted;

        public JumpStartManager(
            ServiceBusOrchestrationService service,
            TimeSpan interval,
            TimeSpan ignoreWindow)
        {
            isStarted = 0;
            this.service = service;
            this.interval = interval;
            this.ignoreWindow = ignoreWindow;
        }

        public Task StartAsync()
        {
            if (Interlocked.CompareExchange(ref isStarted, 1, 0) != 0)
            {
                throw TraceHelper.TraceException(
                    TraceEventType.Error,
                    "JumpStartManager-AlreadyStarted",
                    new InvalidOperationException("JumpStart has already started"));
            }

            TraceHelper.Trace(TraceEventType.Information, "JumpStartManager-Starting", "Jump start manager starting.");
            return Task.Factory.StartNew(() => JumpStartAsync());
        }

        public Task StopAsync()
        {
            if (Interlocked.CompareExchange(ref isStarted, 0, 1) != 1)
            {
                // idempotent
                return Task.FromResult(0);
            }

            TraceHelper.Trace(TraceEventType.Information, "JumpStartManager-Stopped",  "Jump start manager stopped.");

            return Task.FromResult(0);
        }

        public async Task JumpStartAsync()
        {
            while (isStarted == 1)
            {
                TimeSpan delay = this.interval;
                try
                {
                    TraceHelper.Trace(TraceEventType.Information, "JumpStartManager-Fetch-Begin",  "Jump start starting fetch");

                    // TODO: Query in batchces and change timeframe only after curent range is finished
                    IEnumerable<OrchestrationJumpStartInstanceEntity> entities = await this.service.InstanceStore.GetJumpStartEntitiesAsync(1000);
                    TraceHelper.Trace(
                        TraceEventType.Information,
                        "JumpStartManager-Fetch-End",
                        $"JumpStartManager: Fetched state entities count: {entities.Count()}");
                    var taskList = new List<Task>();
                    entities.ToList().ForEach(e => taskList.Add(this.JumpStartOrchestrationAsync(e)));
                    await Task.WhenAll(taskList);
                }
                catch (TimeoutException)
                {
                    delay = this.intervalOnTimeout;
                }
                catch (TaskCanceledException exception)
                {
                    TraceHelper.Trace(
                        TraceEventType.Information,
                        "JumpStartManager-Fetch-TaskCanceled",
                        $"JumpStartManager: TaskCanceledException while fetching state entities, should be harmless: {exception.Message}");
                    delay = this.interval;
                }
                catch (Exception exception) when (!Utils.IsFatal(exception))
                {
                    if (isStarted == 0)
                    {
                        TraceHelper.Trace(
                            TraceEventType.Information,
                            "JumpStartManager-Fetch-HarmlessException",
                            $"JumpStartManager: Harmless exception while fetching state entities after Stop(): {exception.Message}");
                    }
                    else
                    {
                        TraceHelper.TraceException(
                            TraceEventType.Warning,
                            "JumpStartManager-Fetch-Exception",
                            exception,
                            "JumpStartManager: Exception while fetching/processing state entities");
                        delay = this.interval;
                    }
                }

                await Task.Delay(delay);
            }
        }

        protected async Task JumpStartOrchestrationAsync(OrchestrationJumpStartInstanceEntity jumpStartEntity)
        {
            var instance = jumpStartEntity.State.OrchestrationInstance;
            OrchestrationStateInstanceEntity stateEntity = (await this.service.InstanceStore.GetEntitiesAsync(instance.InstanceId, instance.ExecutionId))?.FirstOrDefault();
            if (stateEntity != null)
            {
                // It seems orchestration started, delete entity from JumpStart table
                await this.service.InstanceStore.DeleteJumpStartEntitiesAsync(new[] { jumpStartEntity });
            }
            else if (!jumpStartEntity.JumpStartTime.IsSet() &&
                jumpStartEntity.State.CreatedTime + this.ignoreWindow < DateTime.UtcNow)
            {
                // JumpStart orchestration
                var startedEvent = new ExecutionStartedEvent(-1, jumpStartEntity.State.Input)
                {
                    Tags = jumpStartEntity.State.Tags,
                    Name = jumpStartEntity.State.Name,
                    Version = jumpStartEntity.State.Version,
                    OrchestrationInstance = jumpStartEntity.State.OrchestrationInstance
                };

                var taskMessage = new TaskMessage
                {
                    OrchestrationInstance = jumpStartEntity.State.OrchestrationInstance,
                    Event = startedEvent
                };

                await this.service.SendTaskOrchestrationMessageAsync(taskMessage);
                TraceHelper.Trace(
                    TraceEventType.Information,
                    "JumpStartManager-SendTaskOrchestrationMessage",
                    $"JumpStartManager: SendTaskOrchestrationMessageAsync({instance.InstanceId}, {instance.ExecutionId}) success!");

                // Now update the JumpStart table
                jumpStartEntity.JumpStartTime = DateTime.UtcNow;
                await this.service.InstanceStore.WriteJumpStartEntitiesAsync(new[] { jumpStartEntity });

                TraceHelper.Trace(
                    TraceEventType.Information,
                    "JumpStartManager-WriteJumpStartEntities",
                    $"JumpStartManager: WriteJumpStartEntitiesAsync({instance.InstanceId}, {instance.ExecutionId}) success!");
            }
        }
    }
}