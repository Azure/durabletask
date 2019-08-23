using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Ambrosia
{
    internal class AmbrosiaBackend :
                Backend.ITaskHub
    {
        // this is a sketch only. Ambrosia NuGet package not yet there, code generation not yet there,
        // so a lot is just commented out for now to keep the build intact.

        private readonly Backend.IHost host;
        internal uint PartitionId { get; }
        internal uint NumberPartitions { get; }
        private Backend.IClient client;
        private Backend.IPartition partition;
        private Immortal immortal;
        private CancellationTokenSource shutdownSource;
        private IDisposable immortalShutdown = null;

        public Guid ClientId { get; private set; }

        public AmbrosiaBackend(Backend.IHost host, uint partitionId, uint numberPartitions)
        {
            this.host = host;
            this.PartitionId = partitionId;
            this.NumberPartitions = numberPartitions;
            this.ClientId = MakePartitionSpecificGuid(partitionId);
        }

        internal Guid MakePartitionSpecificGuid(uint partition)
        {
            var bytes = new byte[16];
            bytes[15] = (byte)(partition & 0xFF);
            partition >>= 8;
            bytes[14] = (byte)(partition & 0xFF);
            partition >>= 8;
            bytes[13] = (byte)(partition & 0xFF);
            partition >>= 8;
            bytes[12] = (byte)(partition & 0xFF);
            return new Guid(bytes);
        }

        internal uint GetPartitionFromGuid(Guid partitionSpecificGuid)
        {
            var bytes = partitionSpecificGuid.ToByteArray();
            return (((((((uint)bytes[12]) << 8) + bytes[13]) << 8) + bytes[14]) << 8) + bytes[15];
        }

        Task<bool> Backend.ITaskHub.ExistsAsync()
        {
            throw new NotImplementedException(); // TODO figure out how this should work
        }

        Task Backend.ITaskHub.CreateAsync()
        {
            throw new NotImplementedException(); // TODO figure out how this should work
        }

        Task Backend.ITaskHub.DeleteAsync()
        {
            throw new NotImplementedException(); // TODO figure out how this should work
        }

        async Task Backend.ITaskHub.StartAsync()
        {
            this.shutdownSource = new CancellationTokenSource();
            this.immortal = new Immortal(this);
            this.client = host.AddClient(this.ClientId, this.immortal);
            // this.immortalShutdown = AmbrosiaFactory.Deploy<IImmortal>(Immortal.GetImmortalName(this.PartitionId), myImmortal, _receivePort, _sendPort))
            await this.immortal.StartupComplete;
            this.partition = host.AddPartition(this.PartitionId, this.immortal.PartitionState, this.immortal);
        }

        Task Backend.ITaskHub.StopAsync()
        {
            this.immortalShutdown.Dispose();

            throw new NotImplementedException(); // TODO figure out how this should work
        }

        internal Task ProcessEvent(Event evt)
        {
            if (evt is ClientEvent clientEvent)
            {
                this.client.Process(clientEvent);
                return Task.CompletedTask;
            }
            else if (evt is PartitionEvent partitionEvent)
            {
                return this.partition.ProcessAsync(partitionEvent);
            }
            else
            {
                throw new InvalidOperationException("invalid event type");
            }
        }
    }
}
