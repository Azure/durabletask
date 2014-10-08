namespace ServiceBusTaskScheduler
{
    using System.Collections.Generic;
    using ServiceBusTaskScheduler.History;

    public class DecisionTask
    {
        public DecisionTask()
        {
        }

        public string TaskToken { get; set; }
        public string Name { get; set; }
        public string Version { get; set; }
        public string Id { get; set; }
        public IEnumerable<HistoryEvent> History { get; set; }
    }
}
