using DurableTask;

namespace FrameworkUnitTests.PrototypeMocks
{
    class SessionsQueue
    {
        // Should create a session if one does not exist
        // Should add to active messages of session
        public void AppendMessage(TaskMessage message)
        {
            
        }

        // Should return a session with some active messages
        // all current messages should be marked for completion in complete call
        public SessionDocument AcceptSession()
        {
            return null;
        }

        // Needs to complete all messages which were given at AcceptSession time and delete them.
        public void CompleteSession(SessionDocument session)
        {
            
        }

        // Implementation requirement, since we are maintaining active sessions in a queue,
        // complete takes out of it and returns, if there is no call to complete within a time
        // (node crash) we should put the session back on the queue
        private void ExpireSessionLock()
        {
            
        }
    }
}
