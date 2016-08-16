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
    using System.Collections.Generic;
    using System.Linq;

    // Todo : Need to make this serializable to disk and preferably immutable
    // Alternately, consider keeping this in memory structure and populate it from a separate data structure acquired from reliable store
    public class SessionDocument
    {
        public string Id;
        public OrchestrationRuntimeState SessionState;
        readonly Queue<LockableTaskMessage> Messages = new Queue<LockableTaskMessage>();

        object syncLock = new object();

        public void AppendMessage(TaskMessage newMessage)
        {
            lock (syncLock)
            {
                Messages.Enqueue(new LockableTaskMessage() {TaskMessage = newMessage});
            }
        }

        public bool AnyActiveMessages()
        {
            lock (syncLock)
            {
                return Messages.Any();
            }
        }

        public List<TaskMessage> GetMessages()
        {
            var result = new List<TaskMessage>();
            lock (syncLock)
            {
                foreach (var m in this.Messages)
                {
                    m.Received = true;
                    result.Add(m.TaskMessage);
                }
            }
            return result;
        }

        public void CompleteLockedMessages()
        {
            lock (syncLock)
            {
                while (Messages.Any())
                {
                    var m = Messages.Peek();
                    if (!m.Received)
                    {
                        return;
                    }
                    Messages.Dequeue();
                }
            }
        }
    }
}
