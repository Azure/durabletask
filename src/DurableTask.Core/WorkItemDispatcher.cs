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

        const int BackOffIntervalOnInvalidOperationSecs = 10;
        const int CountDownToZeroDelay = 5;

        // ReSharper disable once StaticMemberInGenericType
        static readonly TimeSpan DefaultReceiveTimeout = TimeSpan.FromSeconds(30);
        readonly string id;
        readonly string name;
        readonly object thisLock = new object();
        readonly SemaphoreSlim initializationLock = new SemaphoreSlim(1, 1);

        volatile int concurrentWorkItemCount;
        volatile int countDownToZeroDelay;
        volatile int delayOverrideSecs;
        volatile int activeFetchers;
        bool isStarted;
        SemaphoreSlim concurrencyLock;
        CancellationTokenSource shutdownCancellationTokenSource;

        /// <summary>
        /// Gets or sets the maximum concurrent work items
        /// </summary>
        public int MaxConcurrentWorkItems { get; set; } = DefaultMaxConcurrentWorkItems;

        /// <summary>
        /// Gets or sets the number of dispatchers to create
        /// </summary>
        public int DispatcherCount { get; set; } = DefaultDispatcherCount;

        readonly Func<T, string> workItemIdentifier;

        Func<TimeSpan, CancellationToken, Task<T>> FetchWorkItem { get; }

        Func<T, Task> ProcessWorkItem { get; }
        
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
        /// Creates a new Work Item Dispatcher with given name and identifier method
        /// </summary>
        /// <param name="name">Name identifying this dispatcher for logging and diagnostics</param>
        /// <param name="workItemIdentifier"></param>
        /// <param name="fetchWorkItem"></param>
        /// <param name="processWorkItem"></param>
        public WorkItemDispatcher(
            string name, 
            Func<T, string> workItemIdentifier,
            Func<TimeSpan, CancellationToken, Task<T>> fetchWorkItem,
            Func<T, Task> processWorkItem
            )
        {
            this.name = name;
            this.id = Guid.NewGuid().ToString("N");
            this.workItemIdentifier = workItemIdentifier ?? throw new ArgumentNullException(nameof(workItemIdentifier));
            FetchWorkItem = fetchWorkItem ?? throw new ArgumentNullException(nameof(fetchWorkItem));
            ProcessWorkItem = processWorkItem ?? throw new ArgumentNullException(nameof(processWorkItem));
        }

        /// <summary>
        /// Starts the work item dispatcher
        /// </summary>
        /// <exception cref="InvalidOperationException">Exception if dispatcher has already been started</exception>
        public async Task StartAsync()
        {
            if (!this.isStarted)
            {
                await this.initializationLock.WaitAsync();
                try
                {
                    if (this.isStarted)
                    {
                        throw TraceHelper.TraceException(TraceEventType.Error, "WorkItemDispatcherStart-AlreadyStarted", new InvalidOperationException($"WorkItemDispatcher '{this.name}' has already started"));
                    }

                    this.concurrencyLock?.Dispose();
                    this.concurrencyLock = new SemaphoreSlim(MaxConcurrentWorkItems);

                    this.shutdownCancellationTokenSource?.Dispose();
                    this.shutdownCancellationTokenSource = new CancellationTokenSource();

                    this.isStarted = true;

                    TraceHelper.Trace(TraceEventType.Information, "WorkItemDispatcherStart", $"WorkItemDispatcher('{this.name}') starting. Id {this.id}.");

                    for (var i = 0; i < DispatcherCount; i++)
                    {
                        string dispatcherId = i.ToString();
                        // We just want this to Run we intentionally don't wait
                        #pragma warning disable 4014
                        Task.Run(() => DispatchAsync(dispatcherId));
                        #pragma warning restore 4014
                    }
                }
                finally
                {
                    this.initializationLock.Release();
                }
            }
        }

        /// <summary>
        /// Stops the work item dispatcher with optional forced flag
        /// </summary>
        /// <param name="forced">Flag indicating whether to stop gracefully and wait for work item completion or just stop immediately</param>
        public async Task StopAsync(bool forced)
        {
            if (!this.isStarted)
            {
                return;
            }

            await this.initializationLock.WaitAsync();
            try
            {
                if (!this.isStarted)
                {
                    return;
                }

                this.isStarted = false;
                this.shutdownCancellationTokenSource.Cancel();

                TraceHelper.Trace(TraceEventType.Information, "WorkItemDispatcherStop-Begin", $"WorkItemDispatcher('{this.name}') stopping. Id {this.id}.");
                if (!forced)
                {
                    var retryCount = 7;
                    //TODO: race condition in graceful shutdown.
                    while ((this.concurrentWorkItemCount > 0 || this.activeFetchers > 0) && retryCount-- >= 0)
                    {
                        TraceHelper.Trace(TraceEventType.Information, "WorkItemDispatcherStop-Waiting", $"WorkItemDispatcher('{this.name}') waiting to stop. Id {this.id}. WorkItemCount: {this.concurrentWorkItemCount}, ActiveFetchers: {this.activeFetchers}");
                        await Task.Delay(4000);
                    }
                }

                TraceHelper.Trace(TraceEventType.Information, "WorkItemDispatcherStop-End", $"WorkItemDispatcher('{this.name}') stopped. Id {this.id}.");
            }
            finally
            {
                this.initializationLock.Release();
            }
        }

        async Task DispatchAsync(string dispatcherId)
        {
            var context = new WorkItemDispatcherContext(this.name, this.id, dispatcherId);

            while (this.isStarted)
            {
                if (!await this.concurrencyLock.WaitAsync(TimeSpan.FromSeconds(5)))
                {
                    TraceHelper.Trace(
                        TraceEventType.Warning,
                        "WorkItemDispatcherDispatch-MaxOperations",
                        GetFormattedLog(dispatcherId, $"Max concurrent operations ({this.concurrentWorkItemCount}) are already in progress. Still waiting for next accept."));
                    continue;
                }

                var delaySecs = 0;
                T workItem = default(T);
                try
                {
                    Interlocked.Increment(ref this.activeFetchers);
                    TraceHelper.Trace(
                        TraceEventType.Verbose, 
                        "WorkItemDispatcherDispatch-StartFetch",
                        GetFormattedLog(dispatcherId, $"Starting fetch with timeout of {DefaultReceiveTimeout} ({this.concurrentWorkItemCount}/{MaxConcurrentWorkItems} max)"));
                    Stopwatch timer = Stopwatch.StartNew();
                    workItem = await FetchWorkItem(DefaultReceiveTimeout, this.shutdownCancellationTokenSource.Token);
                    TraceHelper.Trace(
                        TraceEventType.Verbose, 
                        "WorkItemDispatcherDispatch-EndFetch",
                        GetFormattedLog(dispatcherId, $"After fetch ({timer.ElapsedMilliseconds} ms) ({this.concurrentWorkItemCount}/{MaxConcurrentWorkItems} max)"));
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
                    delaySecs = this.GetDelayInSecondsAfterOnFetchException(exception);
                }
                catch (Exception exception)
                {
                    if (!this.isStarted)
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
                        delaySecs = this.GetDelayInSecondsAfterOnFetchException(exception);
                    }
                }
                finally
                {
                    Interlocked.Decrement(ref this.activeFetchers);
                }

                var scheduledWorkItem = false;
                if (!(Equals(workItem, default(T))))
                {
                    if (!this.isStarted)
                    {
                        if (this.SafeReleaseWorkItem != null)
                        {
                            await this.SafeReleaseWorkItem(workItem);
                        }
                    }
                    else
                    {
                        Interlocked.Increment(ref this.concurrentWorkItemCount);
                        // We just want this to Run we intentionally don't wait
                        #pragma warning disable 4014 
                        Task.Run(() => ProcessWorkItemAsync(context, workItem));
                        #pragma warning restore 4014

                        scheduledWorkItem = true;
                    }
                }

                delaySecs = Math.Max(this.delayOverrideSecs, delaySecs);
                if (delaySecs > 0)
                {
                    await Task.Delay(TimeSpan.FromSeconds(delaySecs));
                }

                if (!scheduledWorkItem)
                {
                    this.concurrencyLock.Release();
                }
            }
        }

        async Task ProcessWorkItemAsync(WorkItemDispatcherContext context, object workItemObj)
        {
            var workItem = (T) workItemObj;
            var abortWorkItem = true;
            string workItemId = string.Empty;

            try
            {
                workItemId = this.workItemIdentifier(workItem);

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
                    "Backing off after invalid operation by " + BackOffIntervalOnInvalidOperationSecs);

                // every time we hit invalid operation exception we back off the dispatcher
                AdjustDelayModifierOnFailure(BackOffIntervalOnInvalidOperationSecs);
            }
            catch (Exception exception) when (!Utils.IsFatal(exception))
            {
                TraceHelper.TraceException(
                    TraceEventType.Error, 
                    "WorkItemDispatcherProcess-Exception", 
                    exception,
                    GetFormattedLog(context.DispatcherId, $"Exception while processing workItem {workItemId}"));

                int delayInSecs = this.GetDelayInSecondsAfterOnProcessException(exception);
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
                    // if the derived dispatcher doesn't think this exception worthy of back-off then
                    // count it as a 'successful' operation
                    AdjustDelayModifierOnSuccess();
                }
            }
            finally
            {
                Interlocked.Decrement(ref this.concurrentWorkItemCount);
                this.concurrencyLock.Release();
            }

            if (abortWorkItem && this.AbortWorkItem != null)
            {
                await ExceptionTraceWrapperAsync(() => this.AbortWorkItem(workItem));
            }

            if (this.SafeReleaseWorkItem != null)
            {
                await ExceptionTraceWrapperAsync(() => this.SafeReleaseWorkItem(workItem));
            }
        }

        void AdjustDelayModifierOnSuccess()
        {
            lock (this.thisLock)
            {
                if (this.countDownToZeroDelay > 0)
                {
                    this.countDownToZeroDelay--;
                }

                if (this.countDownToZeroDelay == 0)
                {
                    this.delayOverrideSecs = 0;
                }
            }
        }

        void AdjustDelayModifierOnFailure(int delaySecs)
        {
            lock (this.thisLock)
            {
                this.delayOverrideSecs = Math.Max(this.delayOverrideSecs, delaySecs);
                this.countDownToZeroDelay = CountDownToZeroDelay;
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
            return $"{this.name}-{this.id}-{dispatcherId}: {message}";
        }

        /// <inheritdoc />
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources.
        /// </summary>
        /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                this.initializationLock.Dispose();
                this.concurrencyLock?.Dispose();
                this.shutdownCancellationTokenSource?.Dispose();
            }
        }
    }
}