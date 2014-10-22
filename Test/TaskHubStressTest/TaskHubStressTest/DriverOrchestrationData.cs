namespace TaskHubStressTest
{
    public class DriverOrchestrationData
    {
        public int NumberOfParallelTasks { get; set; }
        public int NumberOfIteration { get; set; }
        public TestOrchestrationData SubOrchestrationData { get; set; }
    }
}
