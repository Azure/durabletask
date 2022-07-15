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
    using System;
    using System.Collections.Generic;
    using DurableTask.Core.History;

    sealed class TimerFiredEventComparer : IComparer<Message<Guid, TaskMessageItem>>
    {
        public static readonly TimerFiredEventComparer Instance = new TimerFiredEventComparer();

        public int Compare(Message<Guid, TaskMessageItem> first, Message<Guid, TaskMessageItem> second)
        {
            if (first == null)
            {
                throw new ArgumentNullException(nameof(first));
            }

            if (second == null)
            {
                throw new ArgumentNullException(nameof(second));
            }

            var firstTimer = first.Value.TaskMessage.Event as TimerFiredEvent;
            var secondTimer = second.Value.TaskMessage.Event as TimerFiredEvent;

            if (firstTimer == null)
            {
                throw new ArgumentException(nameof(first));
            }
            if (secondTimer == null)
            {
                throw new ArgumentException(nameof(second));
            }

            int firedAtCompareValue = DateTime.Compare(firstTimer.FireAt, secondTimer.FireAt);
            if (firedAtCompareValue != 0)
            {
                return firedAtCompareValue;
            }

            return first.Key.CompareTo(second.Key);
        }
    }
}
