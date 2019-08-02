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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DurableTask.EventHubs
{
    internal class EmulatedStorage : Storage.IPartitionState
    {
        public ClocksState Clocks { get; private set; }

        public ReassemblyState Reassembly { get; private set; }

        public OutboxState Outbox { get; private set; } 

        public TimersState Timers { get; private set; }

        public ActivitiesState Activities { get; private set; }

        public SessionsState Sessions { get; private set; }

        private ConcurrentDictionary<string, InstanceState> instances;

        private ConcurrentDictionary<string, HistoryState> histories;

        private Partition partition;

        private Func<string, InstanceState> instanceFactory;
        private Func<string, HistoryState> historyFactory;

        public EmulatedStorage()
        {
            this.Clocks = new ClocksState();
            this.Reassembly = new ReassemblyState();
            this.Outbox = new OutboxState();
            this.Timers = new TimersState();
            this.Activities = new ActivitiesState();
            this.Sessions = new SessionsState();
            this.instances = new ConcurrentDictionary<string, InstanceState>();
            this.histories = new ConcurrentDictionary<string, HistoryState>();
            this.instanceFactory = MakeInstance;
            this.historyFactory = MakeHistory;
        }

        public InstanceState GetInstance(string instanceId)
        {
            return instances.GetOrAdd(instanceId, this.instanceFactory);
        }

        public HistoryState GetHistory(string instanceId)
        {
            return histories.GetOrAdd(instanceId, this.historyFactory);
        }

        private InstanceState MakeInstance(string instanceId)
        {
            var instance = new InstanceState();
            instance.InstanceId = instanceId;
            instance.Restore(this.partition);
            return instance;
        }

        private HistoryState MakeHistory(string instanceId)
        {
            var history = new HistoryState();
            history.InstanceId = instanceId;
            history.Restore(this.partition);
            return history;
        }

        public Task<long> RestoreAsync(Partition partition)
        {
            this.partition = partition;

            long nextToProcess = 0;

            foreach(var trackedObject in this.GetTrackedObjects())
            {
                long lastProcessed = trackedObject.Restore(partition);

                if (lastProcessed > nextToProcess)
                {
                    nextToProcess = lastProcessed;
                }
            }

            return Task.FromResult(nextToProcess);
        }

        private IEnumerable<TrackedObject> GetTrackedObjects()
        {
            yield return Clocks;
            yield return Reassembly;
            yield return Outbox;
            yield return Timers;
            yield return Activities;
            yield return Sessions;

            foreach(var kvp in instances)
            {
                yield return kvp.Value;
            }
            foreach (var kvp in histories)
            {
                yield return kvp.Value;
            }
        }

        public Task ShutdownAsync()
        {
            return Task.Delay(10);
        }

        // reuse these collection objects between updates (note that updates are never concurrent by design)
        TrackedObject.EffectTracker tracker = new TrackedObject.EffectTracker();

        public void Process(PartitionEvent evt)
        {
            var target = evt.StartProcessingOnObject(this);
            target.ProcessRecursively(evt, tracker);
            tracker.Clear();
        }

        public Task<TResult> ReadAsync<TResult>(Func<TResult> read)
        {
            if (!(read.Target is TrackedObject to))
            {
                throw new ArgumentException("Target must be a tracked object.", nameof(read));
            }
            lock (to.Lock)
            {
                return Task.FromResult(read());
            }
        }

        public Task<TResult> ReadAsync<TArgument1, TResult>(Func<TArgument1, TResult> read, TArgument1 argument)
        {
            if (!(read.Target is TrackedObject to))
            {
                throw new ArgumentException("Target must be a tracked object.", nameof(read));
            }
            lock (to.Lock)
            {
                return Task.FromResult(read(argument));
            }
        }

    }
}
