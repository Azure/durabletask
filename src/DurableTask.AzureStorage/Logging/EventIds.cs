namespace DurableTask.AzureStorage.Logging
{
    static class EventIds
    {
        public const int SendingMessage = 101;
        public const int ReceivedMessage = 102;
        public const int DeletingMessage = 103;
        public const int AbandoningMessage = 104;
        public const int AssertFailure = 105;
        public const int MessageGone = 106;
        public const int GeneralError = 107;
        public const int DuplicateMessageDetected = 108;
        public const int PoisonMessageDetected = 109;
        public const int FetchedInstanceHistory = 110;
        public const int AppendedInstanceHistory = 111;
        public const int OrchestrationServiceStats = 112;
        public const int RenewingMessage = 113;
        public const int MessageFailure = 114;
        public const int OrchestrationProcessingFailure = 115;
        public const int PendingOrchestratorMessageLimitReached = 116;
        public const int WaitingForMoreMessages = 117;
        public const int ReceivedOutOfOrderMessage = 118;
        public const int PartitionManagerInfo = 120;
        public const int PartitionManagerWarning = 121;
        public const int PartitionManagerError = 122;
        public const int StartingLeaseRenewal = 123;
        public const int LeaseRenewalResult = 124;
        public const int LeaseRenewalFailed = 125;
        public const int LeaseAcquisitionStarted = 126;
        public const int LeaseAcquisitionSucceeded = 127;
        public const int LeaseAcquisitionFailed = 128;
        public const int AttemptingToStealLease = 129;
        public const int LeaseStealingSucceeded = 130;
        public const int LeaseStealingFailed = 131;
        public const int PartitionRemoved = 132;
        public const int LeaseRemoved = 133;
        public const int LeaseRemovalFailed = 134;
        public const int InstanceStatusUpdate = 135;
        public const int FetchedInstanceStatus = 136;
        public const int GeneralWarning = 137;
        public const int SplitBrainDetected = 138;
        public const int DiscardingWorkItem = 139;
        public const int ProcessingMessage = 140;
        public const int PurgeInstanceHistory = 141;
    }
}