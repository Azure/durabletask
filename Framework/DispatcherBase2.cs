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

    // TODO : This class need to be refactored

    public abstract class DispatcherBase2<T>
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
        readonly WorkItemIdentifier workItemIdentifier;

        volatile int concurrentWorkItemCount;
        volatile int countDownToZeroDelay;
        volatile int delayOverrideSecs;
        volatile bool isFetching = false;
        private bool isStarted = false;

        protected int maxConcurrentWorkItems;

        /// <summary>
        /// Creates a new Work Item Dispatcher with givne name and identifier method
        /// </summary>
        /// <param name="name">Name identifying this dispatcher for logging and diagnostics</param>
        /// <param name="workItemIdentifier"></param>
        protected DispatcherBase2(string name, WorkItemIdentifier workItemIdentifier)
        {
            this.name = name;
            id = Guid.NewGuid().ToString("N");
            maxConcurrentWorkItems = DefaultMaxConcurrentWorkItems;
            this.workItemIdentifier = workItemIdentifier;
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

                    TraceHelper.Trace(TraceEventType.Information, $"{name} starting. Id {id}.");
                    await Task.Factory.StartNew(() => DispatchAsync());
                }
                finally
                {
                    slimLock.Release();
                }
            }
        }

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


                TraceHelper.Trace(TraceEventType.Information, $"{name} stopping. Id {id}.");
                if (!forced)
                {
                    int retryCount = 7;
                    while ((concurrentWorkItemCount > 0 || isFetching) && retryCount-- >= 0)
                    {
                        Thread.Sleep(4000);
                    }
                }

                TraceHelper.Trace(TraceEventType.Information, $"{name} stopping. Id {id}.");
            }
            finally
            {
                slimLock.Release();
            }
        }

        protected abstract Task<T> OnFetchWorkItemAsync(TimeSpan receiveTimeout);
        protected abstract Task OnProcessWorkItemAsync(T workItem);
        protected abstract Task SafeReleaseWorkItemAsync(T workItem);
        protected abstract Task AbortWorkItemAsync(T workItem);
        protected abstract int GetDelayInSecondsAfterOnFetchException(Exception exception);
        protected abstract int GetDelayInSecondsAfterOnProcessException(Exception exception);

        // TODO : dispatcher refactoring between worker, orchestrator, tacker & DLQ
        public async Task DispatchAsync()
        {
            while (isStarted)
            {
                if (concurrentWorkItemCount >= maxConcurrentWorkItems)
                {
                    TraceHelper.Trace(TraceEventType.Information,
                        GetFormattedLog($"Max concurrent operations are already in progress. Waiting for {WaitIntervalOnMaxSessions}s for next accept."));
                    await Task.Delay(WaitIntervalOnMaxSessions);
                    continue;
                }

                int delaySecs = 0;
                T workItem = default(T);
                try
                {
                    isFetching = true;
                    TraceHelper.Trace(TraceEventType.Information,
                        GetFormattedLog($"Starting fetch with timeout of {DefaultReceiveTimeout}"));
                    workItem = await OnFetchWorkItemAsync(DefaultReceiveTimeout);
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
                            GetFormattedLog("Exception while fetching workItem"));
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
                        await SafeReleaseWorkItemAsync(workItem);
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

        protected virtual async Task ProcessWorkItemAsync(object workItemObj)
        {
            var workItem = (T) workItemObj;
            bool abortWorkItem = true;
            string workItemId = string.Empty;

            try
            {
                workItemId = workItemIdentifier(workItem);

                TraceHelper.Trace(TraceEventType.Information,
                    GetFormattedLog($"Starting to process workItem {workItemId}"));

                await OnProcessWorkItemAsync(workItem);

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

            if (abortWorkItem)
            {
                await ExceptionTraceWrapperAsync(() => AbortWorkItemAsync(workItem));
            }

            await ExceptionTraceWrapperAsync(() => SafeReleaseWorkItemAsync(workItem));
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

        protected string GetFormattedLog(string message)
        {
            return $"{name}-{id}: {message}";
        }

        protected delegate string WorkItemIdentifier(T workItem);
    }
}