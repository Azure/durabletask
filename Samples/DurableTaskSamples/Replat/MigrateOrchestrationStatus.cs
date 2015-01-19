namespace DurableTaskSamples.Replat
{
    using System.Collections.ObjectModel;

    public class MigrateOrchestrationStatus
    {
        public MigrateOrchestrationStatus()
        {
            this.ApplicationsMigrated = new Collection<Application>();
            this.ApplicationsFailed = new Collection<Application>();
        }

        public bool TtlUpdated { get; set; }
        public bool TtlUpdateTimerFired { get; set; }
        public int TotalApplication { get; set; }
        public bool IsMigrated { get; set; }
        public bool IsFlipped { get; set; }
        public bool IsWhitelisted { get; set; }
        public bool IsCleaned { get; set; }

        public bool IsSuccess
        {
            get
            {
                return this.IsMigrated && this.IsFlipped && this.IsWhitelisted;
            }
        }

        public Collection<Application> ApplicationsMigrated { get; private set; }
        public Collection<Application> ApplicationsFailed { get; private set; }
    }
}
