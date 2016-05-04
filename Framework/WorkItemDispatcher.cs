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

namespace DurableTask
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Common;
    using DurableTask.Exceptions;
    using Tracing;

    /// <summary>
    /// Dispatcher class for fetching and processing work items of the supplied type
    /// </summary>
    /// <typeparam name="T">The typed Object to dispatch</typeparam>
    public class WorkItemDispatcher<T>
    {
        const int DefaultMaxConcurrentWorkItems = 20;

        const int BackoffIntervalOnInvalidOperationSecs = 10;
        const int CountDownToZeroDelay = 5;
        static TimeSpan DefaultReceiveTimeout = TimeSpan.FromSeconds(30);
        static TimeSpan WaitIntervalOnMaxSessions = TimeSpan.FromSeconds(5);
        readonly string id;
        readonly string name;
        readonly object thisLock = new object();
        readonly SemaphoreSlim slimLock = new SemaphoreSlim(1, 1);

        volatile int concurrentWorkItemCount;
        volatile int countDownToZeroDelay;
        volatile int delayOverrideSecs;
        volatile bool isFetching = false;
        bool isStarted = false;

        /// <summary>
        /// Gets or sets the maximum concurrent work items
        /// </summary>
        public int MaxConcurrentWorkItems { get; set; }

        readonly Func<T, string> workItemIdentifier;

        readonly Func<TimeSpan, Task<T>> FetchWorkItem;
        readonly Func<T, Task> ProcessWorkItem;
        
        /// <summary>
        /// Method to execute for safely releasing a work item
        /// </summary>
        public Func<T, Task> SafeReleaseWorkItem;

        /// <summary>
        /// Method to execute for aborting a work item
        /// </summary>
        public Func<T, Task> AbortWorkItem;

        /// <summary>
        /// Method to get a delay to wait after a fetch exception
        /// </summary>
        public Func<Exception, int> GetDelayInSecondsAfterOnFetchException = (exception) => 0;

        /// <summary>
        /// Method to get a delay to wait after a process exception
        /// </summary>
        public Func<Exception, int> GetDelayInSecondsAfterOnProcessException = (exception) => 0;

        /// <summary>
        /// Creates a new Work Item Dispatcher with givne name and identifier method
        /// </summary>
        /// <param name="name">Name identifying this dispatcher for logging and diagnostics</param>
        /// <param name="workItemIdentifier"></param>
        /// <param name="fetchWorkItem"></param>
        /// <param name="processWorkItem"></param>
        public WorkItemDispatcher(
            string name, 
            Func<T, string> workItemIdentifier,
            Func<TimeSpan, Task<T>> fetchWorkItem,
            Func<T, Task> processWorkItem
            )
        {
            if (workItemIdentifier == null)
            {
                throw new ArgumentNullException(nameof(workItemIdentifier));
            }

            if (fetchWorkItem == null)
            {
                throw new ArgumentNullException(nameof(fetchWorkItem));
            }

            if (processWorkItem == null)
            {
                throw new ArgumentNullException(nameof(processWorkItem));
            }

            this.name = name;
            id = Guid.NewGuid().ToString("N");
            MaxConcurrentWorkItems = DefaultMaxConcurrentWorkItems;
            this.workItemIdentifier = workItemIdentifier;
            this.FetchWorkItem = fetchWorkItem;
            this.ProcessWorkItem = processWorkItem;
        }

        /// <summary>
        /// Starts the workitem dispatcher
        /// </summary>
        /// <exception cref="InvalidOperationException">Exception if dispatcher has already been started</exception>
        public async Task StartAsync()
        {
            if (!isStarted)
            {
                await slimLock.WaitAsync();
                try
                {
                    if (isStarted)
                    {
                        throw TraceHelper.TraceException(TraceEventType.Error, new InvalidOperationException($"WorkItemDispatcher '{name}' has already started"));
                    }

                    isStarted = true;

                    TraceHelper.Trace(TraceEventType.Information, $"WorkItemDispatcher('{name}') starting. Id {id}.");
                    await Task.Factory.StartNew(() => DispatchAsync());
                }
                finally
                {
                    slimLock.Release();
                }
            }
        }

        /// <summary>
        /// Stops the work item dispatcher with optional forced flag
        /// </summary>
        /// <param name="forced">Flag indicating whether to stop gracefully and wait for work item completion or just stop immediately</param>
        public async Task StopAsync(bool forced)
        {
            if (!isStarted)
            {
                return;
            }

            await slimLock.WaitAsync();
            try
            {
                if (!isStarted)
                {
                    return;
                }

                isStarted = false;


                TraceHelper.Trace(TraceEventType.Information, $"WorkItemDispatcher('{name}') stopping. Id {id}.");
                if (!forced)
                {
                    int retryCount = 7;
                    while ((concurrentWorkItemCount > 0 || isFetching) && retryCount-- >= 0)
                    {
                        await Task.Delay(4000);
                    }
                }

                TraceHelper.Trace(TraceEventType.Information, $"WorkItemDispatcher('{name}') stopped. Id {id}.");
            }
            finally
            {
                slimLock.Release();
            }
        }

        async Task DispatchAsync()
        {
            while (isStarted)
            {
                if (concurrentWorkItemCount >= MaxConcurrentWorkItems)
                {
                    TraceHelper.Trace(TraceEventType.Information,
                        GetFormattedLog($"Max concurrent operations ({concurrentWorkItemCount}) are already in progress. Waiting for {WaitIntervalOnMaxSessions}s for next accept."));
                    await Task.Delay(WaitIntervalOnMaxSessions);
                    continue;
                }

                int delaySecs = 0;
                T workItem = default(T);
                try
                {
                    isFetching = true;
                    TraceHelper.Trace(TraceEventType.Information,
                        GetFormattedLog($"Starting fetch with timeout of {DefaultReceiveTimeout} ({concurrentWorkItemCount}/{MaxConcurrentWorkItems} max)"));
                    var timer = Stopwatch.StartNew();
                    workItem = await FetchWorkItem(DefaultReceiveTimeout);
                    TraceHelper.Trace(TraceEventType.Information,
                        GetFormattedLog($"After Fetch ({timer.ElapsedMilliseconds} ms) ({concurrentWorkItemCount}/{MaxConcurrentWorkItems} max)"));
                }
                catch (TimeoutException)
                {
                    delaySecs = 0;
                }
                catch (TaskCanceledException exception)
                {
                    TraceHelper.Trace(TraceEventType.Information,
                        GetFormattedLog($"TaskCanceledException while fetching workItem, should be harmless: {exception.Message}"));
                    delaySecs = GetDelayInSecondsAfterOnFetchException(exception);
                }
                catch (Exception exception)
                {
                    if (!isStarted)
                    {
                        TraceHelper.Trace(TraceEventType.Information,
                            GetFormattedLog($"Harmless exception while fetching workItem after Stop(): {exception.Message}"));
                    }
                    else
                    {
                        // TODO : dump full node context here
                        TraceHelper.TraceException(TraceEventType.Warning, exception,
                            GetFormattedLog($"Exception while fetching workItem: {exception.Message}"));
                        delaySecs = GetDelayInSecondsAfterOnFetchException(exception);
                    }
                }
                finally
                {
                    isFetching = false;
                }

                if (!(Equals(workItem, default(T))))
                {
                    if (!isStarted)
                    {
                        if (SafeReleaseWorkItem != null)
                        {
                            await SafeReleaseWorkItem(workItem);
                        }
                    }
                    else
                    {
                        Interlocked.Increment(ref concurrentWorkItemCount);
                        Task.Factory.StartNew<Task>(ProcessWorkItemAsync, workItem);
                    }
                }

                delaySecs = Math.Max(delayOverrideSecs, delaySecs);
                if (delaySecs > 0)
                {
                    await Task.Delay(TimeSpan.FromSeconds(delaySecs));
                }
            }
        }

        async Task ProcessWorkItemAsync(object workItemObj)
        {
            var workItem = (T) workItemObj;
            bool abortWorkItem = true;
            string workItemId = string.Empty;

            try
            {
                workItemId = workItemIdentifier(workItem);

                TraceHelper.Trace(TraceEventType.Information,
                    GetFormattedLog($"Starting to process workItem {workItemId}"));

                await ProcessWorkItem(workItem);

                AdjustDelayModifierOnSuccess();

                TraceHelper.Trace(TraceEventType.Information,
                    GetFormattedLog($"Finished processing workItem {workItemId}"));

                abortWorkItem = false;
            }
            catch (TypeMissingException exception)
            {
                TraceHelper.TraceException(TraceEventType.Error, exception,
                    GetFormattedLog($"Exception while processing workItem {workItemId}"));
                TraceHelper.Trace(TraceEventType.Error,
                    "Backing off after invalid operation by " + BackoffIntervalOnInvalidOperationSecs);

                // every time we hit invalid operation exception we back off the dispatcher
                AdjustDelayModifierOnFailure(BackoffIntervalOnInvalidOperationSecs);
            }
            catch (Exception exception) when (!Utils.IsFatal(exception))
            {
                TraceHelper.TraceException(TraceEventType.Error, exception,
                    GetFormattedLog($"Exception while processing workItem {workItemId}"));

                int delayInSecs = GetDelayInSecondsAfterOnProcessException(exception);
                if (delayInSecs > 0)
                {
                    TraceHelper.Trace(TraceEventType.Error,
                        "Backing off after exception by at least " + delayInSecs + " until " + CountDownToZeroDelay +
                        " successful operations");

                    AdjustDelayModifierOnFailure(delayInSecs);
                }
                else
                {
                    // if the derived dispatcher doesn't think this exception worthy of backoff then
                    // count it as a 'successful' operation
                    AdjustDelayModifierOnSuccess();
                }
            }
            finally
            {
                Interlocked.Decrement(ref concurrentWorkItemCount);
            }

            if (abortWorkItem && AbortWorkItem != null)
            {
                await ExceptionTraceWrapperAsync(() => AbortWorkItem(workItem));
            }

            if (SafeReleaseWorkItem != null)
            {
                await ExceptionTraceWrapperAsync(() => SafeReleaseWorkItem(workItem));
            }
        }

        void AdjustDelayModifierOnSuccess()
        {
            lock (thisLock)
            {
                if (countDownToZeroDelay > 0)
                {
                    countDownToZeroDelay--;
                }

                if (countDownToZeroDelay == 0)
                {
                    delayOverrideSecs = 0;
                }
            }
        }

        void AdjustDelayModifierOnFailure(int delaySecs)
        {
            lock (thisLock)
            {
                delayOverrideSecs = Math.Max(delayOverrideSecs, delaySecs);
                countDownToZeroDelay = CountDownToZeroDelay;
            }
        }

        async Task ExceptionTraceWrapperAsync(Func<Task> asyncAction)
        {
            try
            {
                await asyncAction();
            }
            catch (Exception exception) when (!Utils.IsFatal(exception))
            {
                // eat and move on 
                TraceHelper.TraceException(TraceEventType.Error, exception);
            }
        }

        /// <summary>
        /// Method for formatting log messages to include dispatcher name and id information
        /// </summary>
        /// <param name="message">The message to format</param>
        /// <returns>The formatted message</returns>
        protected string GetFormattedLog(string message)
        {
            return $"{name}-{id}: {message}";
        }
    }
}