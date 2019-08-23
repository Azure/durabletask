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

using System;
using System.Collections.Generic;
using System.Text;
using DurableTask.Core;

namespace DurableTask.EventSourced
{
    internal class ActivityWorkItem : TaskActivityWorkItem
    {
        public Partition Partition;

        public long ActivityId;

        public ActivityWorkItem(Partition partition, long activityId, TaskMessage message)
        {
            this.Partition = partition;
            this.ActivityId = activityId;
            this.Id = activityId.ToString();
            this.LockedUntilUtc = DateTime.MaxValue;
            this.TaskMessage = message;
        }
    }
}
