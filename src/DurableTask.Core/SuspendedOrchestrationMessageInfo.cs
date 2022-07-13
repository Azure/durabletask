using System;
using System.Collections.Generic;
using System.Text;
using DurableTask.Core.History;

namespace DurableTask.Core
{
    class SuspendedOrchestrationMessageInfo
    {
        public HistoryEvent historyEvent;

        // Had to put this here to handle raised event case. Otherwise, if I did it in TaskOrchestrationExecutor, I can't access the OpenTasks.
        public TaskOrchestration taskOrchestration;
        public bool skipCarryOverEvents;

        public SuspendedOrchestrationMessageInfo(HistoryEvent historyEvent)
        {
            this.historyEvent = historyEvent;
        }

        public SuspendedOrchestrationMessageInfo(HistoryEvent historyEvent, TaskOrchestration taskOrchestration, bool skipCarryOverEvents)
        {
            this.historyEvent = historyEvent;
            this.taskOrchestration = taskOrchestration;
            this.skipCarryOverEvents = skipCarryOverEvents;
        }
    }
}
