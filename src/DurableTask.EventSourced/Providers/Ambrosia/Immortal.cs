using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Ambrosia
{
    [DataContract]
    internal class Immortal : Backend.ISender // IImmortal, Immortal<IServerProxy>
    {
        [IgnoreDataMember]
        private AmbrosiaBackend backend;

        [DataMember]
        private MemoryStorage state = new MemoryStorage();

        [IgnoreDataMember]
        public Storage.IPartitionState PartitionState => this.state;

        [IgnoreDataMember]
        private TaskCompletionSource<bool> startupComplete;

        [IgnoreDataMember]
        public Task StartupComplete => this.startupComplete.Task;

        //[DataMember]
        //private IImmortalProxy[] proxies;

        public Immortal(AmbrosiaBackend backend)
        {
            this.backend = backend;
            this.startupComplete = new TaskCompletionSource<bool>();
        }

        public static string GetImmortalName(uint partitionId)
        {
            return $"Partition{partitionId:D3}";
        }

        //protected override async Task<bool> OnFirstStart()
        //{
        //    for (int i = 0; i < backend.NumberPartitions; i++)
        //    {
        //        proxies[i] = GetProxy<IImmortalProxy>(GetImmortalName(i));
        //    }
        //    return true; // TODO find out what this return value means
        //}

        //protected override void BecomingPrimary()
        //{
        //    this.startupComplete.TrySetResult(true);
        //}

        public Task ProcessImpulseEvent(Event evt)
        {
            return backend.ProcessEvent(evt);
        }

        public Task ProcessOrderedEvent(Event evt)
        {
            return backend.ProcessEvent(evt);
        }

        void Backend.ISender.Submit(Event evt, Backend.ISendConfirmationListener listener)
        {
            if (evt is ClientEvent clientEvent)
            {
                var clientId = clientEvent.ClientId;
                var partitionId = backend.GetPartitionFromGuid(clientId);
                this.SendImpulseEvent(partitionId, evt);
            }
            else if (evt is TaskMessageReceived taskMessageReceivedEvent)
            {
                var partitionId = taskMessageReceivedEvent.PartitionId;
                this.SendOrderedEvent(partitionId, evt);
            }
            else if (evt is PartitionEvent partitionEvent)
            {
                var partitionId = partitionEvent.PartitionId;
                this.SendOrderedEvent(partitionId, evt);
            }
            listener?.ConfirmDurablySent(evt);
        }

        private void SendImpulseEvent(uint partitionId, Event evt)
        {
            //proxies[partitionId].ImpulseEventFork(evt);
        }

        private void SendOrderedEvent(uint partitionId, Event evt)
        {
            //proxies[partitionId].OrderedEventFork(evt);
        }

    
    }
}
