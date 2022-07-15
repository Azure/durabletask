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
    using System;

    /// <summary>
    /// Autostart-Orchestration instance creator for a type, prefixes type name with '@'
    /// </summary>
    /// <typeparam name="T">Type of Orchestration</typeparam>
    public class AutoStartOrchestrationCreator : ObjectCreator<TaskOrchestration>
    {
        readonly TaskOrchestration instance;
        readonly Type prototype;

        /// <summary>
        /// Creates a new AutoStartOrchestrationCreator of supplied type
        /// </summary>
        /// <param name="type">Type to use for the creator</param>
        public AutoStartOrchestrationCreator(Type type)
        {
            this.prototype = type;
            Initialize(type);
        }

        /// <summary>
        /// Creates a new AutoStartOrchestrationCreator using type of supplied object instance
        /// </summary>
        /// <param name="instance">Object instances to infer the type from</param>
        public AutoStartOrchestrationCreator(TaskOrchestration instance)
        {
            this.instance = instance;
            Initialize(instance);
        }

        ///<inheritdoc/>
        public override TaskOrchestration Create()
        {
            if (this.prototype != null)
            {
                return (TaskOrchestration)Activator.CreateInstance(this.prototype);
            }

            return this.instance;
        }

        void Initialize(object obj)
        {
            Name = $"@{NameVersionHelper.GetDefaultName(obj)}";
            Version = NameVersionHelper.GetDefaultVersion(obj);
        }
    }
}