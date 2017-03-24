//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace DurableTask.AzureStorage
{
    using System;
    using DurableTask.History;

    public class HistoryEventArgs
    {
        internal HistoryEventArgs(HistoryEvent newHistoryEvent, OrchestrationRuntimeState state)
        {
            if (newHistoryEvent == null)
            {
                throw new ArgumentNullException(nameof(newHistoryEvent));
            }

            if (state == null)
            {
                throw new ArgumentNullException(nameof(state));
            }

            this.NewHistoryEvent = newHistoryEvent;
            this.InstanceId = state.OrchestrationInstance.InstanceId;
            this.State = state;
        }

        internal HistoryEventArgs(HistoryEvent newHistoryEvent, string instanceId)
        {
            if (newHistoryEvent == null)
            {
                throw new ArgumentNullException(nameof(newHistoryEvent));
            }

            if (string.IsNullOrEmpty(instanceId))
            {
                throw new ArgumentNullException(nameof(instanceId));
            }

            this.NewHistoryEvent = newHistoryEvent;
            this.InstanceId = instanceId;
        }

        public HistoryEvent NewHistoryEvent { get; private set; }

        public string InstanceId { get; private set; }

        public OrchestrationRuntimeState State { get; private set; }
    }
}
