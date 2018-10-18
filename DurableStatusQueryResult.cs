using System;

public class DurableStatusQueryResult
{
	public DurableStatusQueryResult()
	{
        // IList<OrchestrationState>, string
        public IEnumerable<OrchestrationState> OrchestrationState { get; set; }
        public string ContinuationToken { get; set; }
    }
}
