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

namespace DurableTask.Emulator
{
    using DurableTask.Core;
    using System.Collections.Generic;

    class TaskSession
    {
        public string Id;
        public byte[] SessionState;
        public List<TaskMessage> Messages;
        public HashSet<TaskMessage> LockTable;

        public TaskSession()
        {
            this.SessionState = null;
            this.Messages = new List<TaskMessage>();
            this.LockTable = new HashSet<TaskMessage>();
        }
    }
}