using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DurableTask;

namespace FrameworkUnitTests.PrototypeMocks
{
    class SessionDocument
    {
        public string Id;
        public byte[] SessionState;
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

        public List<TaskMessage> LockMessages()
        {
            var result = new List<TaskMessage>();
            lock (syncLock)
            {
                foreach (var m in this.Messages)
                {
                    m.Locked = true;
                    result.Add(m.TaskMessage);
                }
            }
            return result;
        }

        public void CompleteLocked()
        {
            lock (syncLock)
            {
                while (Messages.Any())
                {
                    var m = Messages.Peek();
                    if (!m.Locked)
                    {
                        return;
                    }
                    Messages.Dequeue();
                }
            }
        }
    }

    class LockableTaskMessage
    {
        public TaskMessage TaskMessage;
        public bool Locked;
    }
}
