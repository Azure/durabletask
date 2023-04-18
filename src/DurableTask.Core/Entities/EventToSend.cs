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
namespace DurableTask.Core.Entities
{
    /// <summary>
    /// The data associated with sending an event to an orchestration.
    /// </summary>
    public readonly struct EventToSend
    {
        /// <summary>
        /// The name of the event.
        /// </summary>
        public readonly string EventName { get; }

        /// <summary>
        /// The content of the event.
        /// </summary>
        public readonly object EventContent { get; }

        /// <summary>
        /// The target instance for the event.
        /// </summary>
        public readonly OrchestrationInstance TargetInstance { get; }

        /// <summary>
        /// Construct an entity message event with the given members.
        /// </summary>
        /// <param name="name">The name of the event.</param>
        /// <param name="content">The content of the event.</param>
        /// <param name="target">The target of the event.</param>
        public EventToSend(string name, object content, OrchestrationInstance target)
        {
            EventName = name;
            EventContent = content;
            TargetInstance = target;
        }
    }
}