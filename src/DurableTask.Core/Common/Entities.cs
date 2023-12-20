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
#nullable enable
namespace DurableTask.Core.Common
{
    using DurableTask.Core.History;
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Helpers for dealing with special naming conventions around auto-started orchestrations (entities)
    /// </summary>
    public static class Entities
    {
        /// <summary>
        /// Determine if a given instance id is an entity (an auto-started orchestration)
        /// </summary>
        /// <param name="instanceId"></param>
        /// <returns></returns>
        public static bool IsEntityInstance(string instanceId)
        {
            return !string.IsNullOrEmpty(instanceId) && instanceId[0] == '@';
        }

        /// <summary>
        /// Determine if a task message is an entity message with a time delay
        /// </summary>
        /// <param name="taskMessage">The task message</param>
        /// <param name="due">The universal time at which this message should be delivered</param>
        /// <returns>true if this is an entity message with a time delay</returns>
        /// <remarks>
        /// We assume that auto-started orchestrations (i.e. instance ids starting with '@') are
        /// used exclusively by durable entities; so we can follow 
        /// a custom naming convention to pass a time parameter.
        /// </remarks>
        public static bool IsDelayedEntityMessage(TaskMessage taskMessage, out DateTime due)
        {
            // Special functionality for entity messages with a delivery delay 
            if (taskMessage.Event is EventRaisedEvent eventRaisedEvent
                && IsEntityInstance(taskMessage.OrchestrationInstance.InstanceId))
            {
                string eventName = eventRaisedEvent.Name;
                if (eventName != null && eventName.Length >= 3 && eventName[2] == '@'
                    && DateTime.TryParse(eventName.Substring(3), out DateTime scheduledTime))
                {
                    due = scheduledTime.ToUniversalTime();
                    return true;
                }
            }

            due = default;
            return false;
        }

        /// <summary>
        /// Tests whether an instance should automatically start if receiving messages and not already being started, and inserts
        /// a corresponding ExecutionStartedEvent taskMessage.
        /// </summary>
        /// <param name="instanceId"></param>
        /// <param name="newMessages"></param>
        /// <returns></returns>
        public static bool AutoStart(string instanceId, IList<TaskMessage> newMessages)
        {
            if (IsEntityInstance(instanceId)
                 && newMessages[0].Event.EventType == EventType.EventRaised
                 && newMessages[0].OrchestrationInstance.ExecutionId == null)
            {
                // automatically start this instance
                var orchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = instanceId,
                    ExecutionId = Guid.NewGuid().ToString("N"),
                };
                var startedEvent = new ExecutionStartedEvent(-1, null)
                {
                    Name = instanceId,
                    Version = "",
                    OrchestrationInstance = orchestrationInstance
                };
                var taskMessage = new TaskMessage()
                {
                    OrchestrationInstance = orchestrationInstance,
                    Event = startedEvent
                };
                newMessages.Insert(0, taskMessage);
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
