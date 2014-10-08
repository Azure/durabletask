namespace Microsoft.ServiceBus.Task.Decision
{

    class ExecutionCompleteDecision : Decision
    {
        public override DecisionType DecisionType
        {
            get { return DecisionType.ExecutionCompleteDecision; }
        }

        public string Result { get; set; }
    }

}
