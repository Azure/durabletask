﻿//  ----------------------------------------------------------------------------------
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


    [Obsolete]
    public abstract class DispatcherBase<T>
    {
        const int DefaultMaxConcurrentWorkItems = 20;

        const int BackoffIntervalOnInvalidOperationSecs = 10;
        const int CountDownToZeroDelay = 5;
        static TimeSpan DefaultReceiveTimeout = TimeSpan.FromSeconds(30);
        static TimeSpan WaitIntervalOnMaxSessions = TimeSpan.FromSeconds(5);
        readonly string id;
        readonly string name;
        readonly object thisLock = new object();
        readonly WorkItemIdentifier workItemIdentifier;

        volatile int concurrentWorkItemCount;
        volatile int countDownToZeroDelay;
        volatile int delayOverrideSecs;
        volatile bool isFetching;
        volatile int isStarted;

        protected int maxConcurrentWorkItems;

        protected DispatcherBase(string name, WorkItemIdentifier workItemIdentifier)
        {
            isStarted = 0;
            isFetching = false;
            this.name = name;
            id = Guid.NewGuid().ToString("N");
            maxConcurrentWorkItems = DefaultMaxConcurrentWorkItems;
            this.workItemIdentifier = workItemIdentifier;
        }

        public void Start()
        {
            if (Interlocked.CompareExchange(ref isStarted, 1, 0) != 0)
            {
                throw TraceHelper.TraceException(TraceEventType.Error,
                    new InvalidOperationException("TaskOrchestrationDispatcher has already started"));
            }

            TraceHelper.Trace(TraceEventType.Information, "{0} starting. Id {1}.", name, id);
            OnStart();
            Task.Factory.StartNew(() => DispatchAsync());
        }

        public void Stop()
        {
            Stop(false);
        }

        public void Stop(bool forced)
        {
            if (Interlocked.CompareExchange(ref isStarted, 0, 1) != 1)
            {
                //idempotent
                return;
            }

            TraceHelper.Trace(TraceEventType.Information, "{0} stopping. Id {1}.", name, id);
            OnStopping(forced);

            if (!forced)
            {
                int retryCount = 7;
                while ((concurrentWorkItemCount > 0 || isFetching) && retryCount-- >= 0)
                {
                    Thread.Sleep(4000);
                }
            }

            OnStopped(forced);
            TraceHelper.Trace(TraceEventType.Information, "{0} stopped. Id {1}.", name, id);
        }

        protected abstract void OnStart();
        protected abstract void OnStopping(bool isForced);
        protected abstract void OnStopped(bool isForced);

        protected abstract Task<T> OnFetchWorkItemAsync(TimeSpan receiveTimeout);
        protected abstract Task OnProcessWorkItemAsync(T workItem);
        protected abstract Task SafeReleaseWorkItemAsync(T workItem);
        protected abstract Task AbortWorkItemAsync(T workItem);
        protected abstract int GetDelayInSecondsAfterOnFetchException(Exception exception);
        protected abstract int GetDelayInSecondsAfterOnProcessException(Exception exception);

        // TODO : dispatcher refactoring between worker, orchestrator, tacker & DLQ
        public async Task DispatchAsync()
        {
            while (isStarted == 1)
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
                    if (isStarted == 0)
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
                    if (isStarted == 0)
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