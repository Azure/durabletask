using System;
using System.Threading.Tasks;

namespace DurableTask.EventHubs
{
    /// <summary>
    /// General pattern for an asynchronous worker that performs a work task, when notified,
    /// to service queued work. Each work cycle handles ALL the queued work. 
    /// If new work arrives during a work cycle, another cycle is scheduled. 
    /// The worker never executes more than one instance of the work cycle at a time, 
    /// and consumes no thread or task resources when idle. It uses TaskScheduler.Current 
    /// to schedule the work cycles.
    /// </summary>
    internal abstract class BatchWorker
    {
        private readonly object lockable = new object();

        private bool startingCurrentWorkCycle;

        // Task for the current work cycle, or null if idle
        private volatile Task currentWorkCycle;

        // Flag is set to indicate that more work has arrived during execution of the task
        private volatile bool moreWork;

        private Action<Task> checkForMoreWorkAction;

        /// <summary>Implement this member in derived classes to define what constitutes a work cycle</summary>
        protected abstract Task Work();

        /// <summary>
        /// Default constructor.
        /// </summary>
        public BatchWorker()
        {
            // store delegate so it does not get newly allocated on each call
            this.checkForMoreWorkAction = (t) => this.CheckForMoreWork(); 
        }

        /// <summary>
        /// Notify the worker that there is more work.
        /// </summary>
        public void Notify()
        {
            lock (lockable)
            {
                if (currentWorkCycle != null || startingCurrentWorkCycle)
                {
                    // lets the current work cycle know that there is more work
                    moreWork = true;
                }
                else
                {
                    // start a work cycle
                    Start();
                }
            }
        }

        private void Start()
        {
            // Indicate that we are starting the worker (to prevent double-starts on self-notify)
            startingCurrentWorkCycle = true;

            try
            {
                // Start the task that is doing the work
                currentWorkCycle = Work();
            }
            finally
            {
                // By now we have started, and stored the task in currentWorkCycle
                startingCurrentWorkCycle = false;

                // chain a continuation that checks for more work, on the same scheduler
                currentWorkCycle.ContinueWith(this.checkForMoreWorkAction, TaskScheduler.Current);
            }
        }

        /// <summary>
        /// Executes at the end of each work cycle on the same task scheduler.
        /// </summary>
        private void CheckForMoreWork()
        {
            lock (lockable)
            {
                if (moreWork)
                {
                    moreWork = false;

                    // start the next work cycle
                    Start();
                }
                else
                {
                    currentWorkCycle = null;
                }
            }
        }
    }
}