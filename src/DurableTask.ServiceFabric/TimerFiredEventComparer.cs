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

namespace DurableTask.ServiceFabric
{
    using System;
    using System.Collections.Generic;
    using DurableTask.History;

    sealed class TimerFiredEventComparer : IComparer<Message<string, TaskMessage>>
    {
        public static readonly TimerFiredEventComparer Instance = new TimerFiredEventComparer();

        public int Compare(Message<string, TaskMessage> first, Message<string, TaskMessage> second)
        {
            var firstTimer = first?.Value.Event as TimerFiredEvent;
            var secondTimer = second?.Value.Event as TimerFiredEvent;

            if (firstTimer == null)
            {
                throw new ArgumentException(nameof(first));
            }
            if (secondTimer == null)
            {
                throw new ArgumentException(nameof(second));
            }

            return DateTime.Compare(firstTimer.FireAt, secondTimer.FireAt);
        }
    }
}
