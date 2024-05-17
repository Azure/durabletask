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

namespace DurableTask.Core.Logging
{
    // WARNING: Changing the *name* OR the *value* of any of these constants is a breaking change!!
    static class EventIds
    {
        public const int TaskHubWorkerStarting = 10;
        public const int TaskHubWorkerStarted = 11;
        public const int TaskHubWorkerStopping = 12;
        public const int TaskHubWorkerStopped = 13;

        public const int DispatcherStarting = 20;
        public const int DispatcherStopped = 21;
        public const int DispatchersStopping = 22;
        public const int FetchWorkItemStarting = 23;
        public const int FetchWorkItemCompleted = 24;
        public const int FetchWorkItemFailure = 25;
        public const int FetchingThrottled = 26;
        public const int ProcessWorkItemStarting = 27;
        public const int ProcessWorkItemCompleted = 28;
        public const int ProcessWorkItemFailed = 29;

        public const int SchedulingOrchestration = 40;
        public const int RaisingEvent = 41;
        public const int TerminatingInstance = 42;
        public const int WaitingForInstance = 43;
        public const int FetchingInstanceState = 44;
        public const int FetchingInstanceHistory = 45;
        public const int SchedulingActivity = 46;
        public const int CreatingTimer = 47;
        public const int SendingEvent = 48;
        public const int OrchestrationCompleted = 49;
        public const int ProcessingOrchestrationMessage = 50;
        public const int OrchestrationExecuting = 51;
        public const int OrchestrationExecuted = 52;
        public const int OrchestrationAborted = 53;
        public const int DiscardingMessage = 54;
        public const int EntityBatchExecuting = 55;
        public const int EntityBatchExecuted = 56;
        public const int EntityLockAcquired = 57;
        public const int EntityLockReleased = 58;

        public const int TaskActivityStarting = 60;
        public const int TaskActivityCompleted = 61;
        public const int TaskActivityFailure = 62;
        public const int TaskActivityAborted = 63;
        public const int TaskActivityDispatcherError = 64;
        public const int RenewActivityMessageStarting = 65;
        public const int RenewActivityMessageCompleted = 66;
        public const int RenewActivityMessageFailed = 67;

        public const int SuspendingInstance = 68;
        public const int ResumingInstance = 69;

        public const int RenewOrchestrationWorkItemStarting = 70;
        public const int RenewOrchestrationWorkItemCompleted = 71;
        public const int RenewOrchestrationWorkItemFailed = 72;

        public const int OrchestrationDebugTrace = 73;
    }
}
