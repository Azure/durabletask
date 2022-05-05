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

namespace DurableTask.AzureServiceFabric
{
    using System.Collections.Generic;
    using DurableTask.Core;

    internal class OrchestrationInstanceComparer : IEqualityComparer<OrchestrationInstance>
    {
        public static readonly OrchestrationInstanceComparer Default = new OrchestrationInstanceComparer();

        public bool Equals(OrchestrationInstance first, OrchestrationInstance second)
        {
            if (first == null || second == null)
            {
                return first == second;
            }

            if (string.Equals(first.InstanceId, second.InstanceId) && string.Equals(first.ExecutionId, second.ExecutionId))
            {
                return true;
            }

            return false;
        }

        public int GetHashCode(OrchestrationInstance instance)
        {
            return instance.GetHashCode();
        }
    }
}
