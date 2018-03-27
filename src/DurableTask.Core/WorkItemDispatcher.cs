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

namespace DurableTask.Core
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core.Common;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.Tracing;

    /// <summary>
    /// Dispatcher class for fetching and processing work items of the supplied type
    /// </summary>
    /// <typeparam name="T">The typed Object to dispatch</typeparam>
    public class WorkItemDispatcher<T> : IDisposable
    {
        const int DefaultMaxConcurrentWorkItems = 20;
        const int DefaultDispatcherCount = 1;

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
        volatile int activeFetchers = 0;
        bool isStarted = false;

        /// <summary>
        /// Gets or sets the maximum concurrent work items
        /// </summary>
        public int MaxConcurrentWorkItems { get; set; } = DefaultMaxConcurrentWorkItems;

        /// <summary>
        /// Gets or sets the number of dispatchers to create
        /// </summary>
        public int DispatcherCount { get; set; } = DefaultDispatcherCount;

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
            this.name = name;
            id = Guid.NewGuid().ToString("N");
            this.workItemIdentifier = workItemIdentifier ?? throw new ArgumentNullException(nameof(workItemIdentifier));
            this.FetchWorkItem = fetchWorkItem ?? throw new ArgumentNullException(nameof(fetchWorkItem));
            this.ProcessWorkItem = processWorkItem ?? throw new ArgumentNullException(nameof(processWorkItem));
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
                        throw TraceHelper.TraceException(TraceEventType.Error, "WorkItemDispatcherStart-AlreadyStarted", new InvalidOperationException($"WorkItemDispatcher '{name}' has already started"));
                    }

                    isStarted = true;

                    TraceHelper.Trace(TraceEventType.Information, "WorkItemDispatcherStart", $"WorkItemDispatcher('{name}') starting. Id {id}.");
                    for (var i = 0; i < DispatcherCount; i++)
                    {
                        var dispatcherId = i.ToString();
                        // We just want this to Run we intentionally don't wait
                        #pragma warning disable 4014
                        Task.Run(() => DispatchAsync(dispatcherId));
                        #pragma warning restore 4014
                    }
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

                TraceHelper.Trace(TraceEventType.Information, "WorkItemDispatcherStop-Begin", $"WorkItemDispatcher('{name}') stopping. Id {id}.");
                if (!forced)
                {
                    int retryCount = 7;
                    while ((concurrentWorkItemCount > 0 || activeFetchers > 0) && retryCount-- >= 0)
                    {
                        TraceHelper.Trace(TraceEventType.Information, "WorkItemDispatcherStop-Waiting", $"WorkItemDispatcher('{name}') waiting to stop. Id {id}. WorkItemCount: {concurrentWorkItemCount}, ActiveFetchers: {activeFetchers}");
                        await Task.Delay(4000);
                    }
                }

                TraceHelper.Trace(TraceEventType.Information, "WorkItemDispatcherStop-End", $"WorkItemDispatcher('{name}') stopped. Id {id}.");
            }
            finally
            {
                slimLock.Release();
            }
        }

        async Task DispatchAsync(string dispatcherId)
        {
            var context = new WorkItemDispatcherContext(name, id, dispatcherId);

            while (isStarted)
            {
                if (concurrentWorkItemCount >= MaxConcurrentWorkItems)
                {
                    TraceHelper.Trace(
                        TraceEventType.Information, 
                        "WorkItemDispatcherDispatch-MaxOperations",
                        GetFormattedLog(dispatcherId, $"Max concurrent operations ({concurrentWorkItemCount}) are already in progress. Waiting for {WaitIntervalOnMaxSessions}s for next accept."));
                    await Task.Delay(WaitIntervalOnMaxSessions);
                    continue;
                }

                int delaySecs = 0;
                T workItem = default(T);
                try
                {
                    Interlocked.Increment(ref activeFetchers);
                    TraceHelper.Trace(
                        TraceEventType.Verbose, 
                        "WorkItemDispatcherDispatch-StartFetch",
                        GetFormattedLog(dispatcherId, $"Starting fetch with timeout of {DefaultReceiveTimeout} ({concurrentWorkItemCount}/{MaxConcurrentWorkItems} max)"));
                    var timer = Stopwatch.StartNew();
                    workItem = await FetchWorkItem(DefaultReceiveTimeout);
                    TraceHelper.Trace(
                        TraceEventType.Verbose, 
                        "WorkItemDispatcherDispatch-EndFetch",
                        GetFormattedLog(dispatcherId, $"After fetch ({timer.ElapsedMilliseconds} ms) ({concurrentWorkItemCount}/{MaxConcurrentWorkItems} max)"));
                }
                catch (TimeoutException)
                {
                    delaySecs = 0;
                }
                catch (TaskCanceledException exception)
                {
                    TraceHelper.Trace(
                        TraceEventType.Information,
                        "WorkItemDispatcherDispatch-TaskCanceledException",
                        GetFormattedLog(dispatcherId, $"TaskCanceledException while fetching workItem, should be harmless: {exception.Message}"));
                    delaySecs = GetDelayInSecondsAfterOnFetchException(exception);
                }
                catch (Exception exception)
                {
                    if (!isStarted)
                    {
                        TraceHelper.Trace(
                            TraceEventType.Information, 
                            "WorkItemDispatcherDispatch-HarmlessException",
                            GetFormattedLog(dispatcherId, $"Harmless exception while fetching workItem after Stop(): {exception.Message}"));
                    }
                    else
                    {
                        // TODO : dump full node context here
                        TraceHelper.TraceException(
                            TraceEventType.Warning, 
                            "WorkItemDispatcherDispatch-Exception", 
                            exception,
                            GetFormattedLog(dispatcherId, $"Exception while fetching workItem: {exception.Message}"));
                        delaySecs = GetDelayInSecondsAfterOnFetchException(exception);
                    }
                }
                finally
                {
                    Interlocked.Decrement(ref activeFetchers);
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
                        // We just want this to Run we intentionally don't wait
                        #pragma warning disable 4014 
                        Task.Run(() => ProcessWorkItemAsync(context, workItem));
                        #pragma warning restore 4014
                    }
                }

                delaySecs = Math.Max(delayOverrideSecs, delaySecs);
                if (delaySecs > 0)
                {
                    await Task.Delay(TimeSpan.FromSeconds(delaySecs));
                }
            }
        }

        async Task ProcessWorkItemAsync(WorkItemDispatcherContext context, object workItemObj)
        {
            var workItem = (T) workItemObj;
            bool abortWorkItem = true;
            string workItemId = string.Empty;

            try
            {
                workItemId = workItemIdentifier(workItem);

                TraceHelper.Trace(
                    TraceEventType.Information, 
                    "WorkItemDispatcherProcess-Begin",
                    GetFormattedLog(context.DispatcherId, $"Starting to process workItem {workItemId}"));

                await ProcessWorkItem(workItem);

                AdjustDelayModifierOnSuccess();

                TraceHelper.Trace(
                    TraceEventType.Information, 
                    "WorkItemDispatcherProcess-End",
                    GetFormattedLog(context.DispatcherId, $"Finished processing workItem {workItemId}"));

                abortWorkItem = false;
            }
            catch (TypeMissingException exception)
            {
                TraceHelper.TraceException(
                    TraceEventType.Error, 
                    "WorkItemDispatcherProcess-TypeMissingException", 
                    exception,
                    GetFormattedLog(context.DispatcherId, $"Exception while processing workItem {workItemId}"));
                TraceHelper.Trace(
                    TraceEventType.Error, 
                    "WorkItemDispatcherProcess-TypeMissingBackingOff",
                    "Backing off after invalid operation by " + BackoffIntervalOnInvalidOperationSecs);

                // every time we hit invalid operation exception we back off the dispatcher
                AdjustDelayModifierOnFailure(BackoffIntervalOnInvalidOperationSecs);
            }
            catch (Exception exception) when (!Utils.IsFatal(exception))
            {
                TraceHelper.TraceException(
                    TraceEventType.Error, 
                    "WorkItemDispatcherProcess-Exception", 
                    exception,
                    GetFormattedLog(context.DispatcherId, $"Exception while processing workItem {workItemId}"));

                int delayInSecs = GetDelayInSecondsAfterOnProcessException(exception);
                if (delayInSecs > 0)
                {
                    TraceHelper.Trace(
                        TraceEventType.Error, 
                        "WorkItemDispatcherProcess-BackingOff",
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
                TraceHelper.TraceException(TraceEventType.Error, "WorkItemDispatcher-ExceptionTraceError", exception);
            }
        }

        /// <summary>
        /// Method for formatting log messages to include dispatcher name and id information
        /// </summary>
        /// <param name="dispatcherId">Id of the dispatcher</param>
        /// <param name="message">The message to format</param>
        /// <returns>The formatted message</returns>
        protected string GetFormattedLog(string dispatcherId, string message)
        {
            return $"{name}-{id}-{dispatcherId}: {message}";
        }

        /// <inheritdoc />
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <inheritdoc />
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                ((IDisposable)slimLock).Dispose();
            }
        }
    }
}