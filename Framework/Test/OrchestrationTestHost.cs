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

namespace DurableTask.Test
{
    using System;
    using System.Diagnostics;
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;

    public class OrchestrationTestHost
    {
        readonly NameVersionObjectManager<TaskActivity> activityObjectManager;
        readonly FakeOrchestrationClock clock;
        readonly FakeOrchestrationExecutor orchestrationExecutor;
        readonly NameVersionObjectManager<TaskOrchestration> orchestrationObjectManager;
        readonly FakeTaskActivityExecutor taskActivityExecutor;
        JsonDataConverter dataConverter;

        public OrchestrationTestHost()
        {
            dataConverter = new JsonDataConverter();
            orchestrationObjectManager = new NameVersionObjectManager<TaskOrchestration>();
            activityObjectManager = new NameVersionObjectManager<TaskActivity>();
            orchestrationExecutor = new FakeOrchestrationExecutor(orchestrationObjectManager);
            taskActivityExecutor = new FakeTaskActivityExecutor(activityObjectManager);
            clock = new FakeOrchestrationClock();
            ClockSpeedUpFactor = 1;
        }

        public int ClockSpeedUpFactor { get; set; }

        public bool WaitingForTimerToFire { get; set; }

        public OrchestrationTestHost AddTaskOrchestrations(params Type[] taskOrchestrationTypes)
        {
            foreach (Type type in taskOrchestrationTypes)
            {
                ObjectCreator<TaskOrchestration> creator = new DefaultObjectCreator<TaskOrchestration>(type);
                orchestrationObjectManager.Add(creator);
            }

            return this;
        }

        public OrchestrationTestHost AddTaskOrchestrations(
            params ObjectCreator<TaskOrchestration>[] taskOrchestrationCreators)
        {
            foreach (var creator in taskOrchestrationCreators)
            {
                orchestrationObjectManager.Add(creator);
            }

            return this;
        }

        public OrchestrationTestHost AddTaskActivities(params TaskActivity[] taskActivityObjects)
        {
            foreach (TaskActivity instance in taskActivityObjects)
            {
                ObjectCreator<TaskActivity> creator = new DefaultObjectCreator<TaskActivity>(instance);
                activityObjectManager.Add(creator);
            }

            return this;
        }

        public OrchestrationTestHost AddTaskActivities(params Type[] taskActivityTypes)
        {
            foreach (Type type in taskActivityTypes)
            {
                ObjectCreator<TaskActivity> creator = new DefaultObjectCreator<TaskActivity>(type);
                activityObjectManager.Add(creator);
            }

            return this;
        }

        public OrchestrationTestHost AddTaskActivities(params ObjectCreator<TaskActivity>[] taskActivityCreators)
        {
            foreach (var creator in taskActivityCreators)
            {
                activityObjectManager.Add(creator);
            }

            return this;
        }

        public OrchestrationTestHost AddTaskActivitiesFromInterface<T>(T activities)
        {
            return AddTaskActivitiesFromInterface(activities, false);
        }

        public OrchestrationTestHost AddTaskActivitiesFromInterface<T>(T activities, bool useFullyQualifiedMethodNames)
        {
            Type @interface = typeof (T);
            if (!@interface.IsInterface)
            {
                throw new Exception("Contract can only be an interface.");
            }

            foreach (MethodInfo methodInfo in @interface.GetMethods())
            {
                TaskActivity taskActivity = new ReflectionBasedTaskActivity(activities, methodInfo);
                ObjectCreator<TaskActivity> creator =
                    new NameValueObjectCreator<TaskActivity>(
                        NameVersionHelper.GetDefaultName(methodInfo, useFullyQualifiedMethodNames),
                        NameVersionHelper.GetDefaultVersion(methodInfo), taskActivity);
                activityObjectManager.Add(creator);
            }
            return this;
        }

        public Task<T> RunOrchestration<T>(Type orchestrationType, object input)
        {
            return RunOrchestration<T>(NameVersionHelper.GetDefaultName(orchestrationType),
                NameVersionHelper.GetDefaultVersion(orchestrationType), input);
        }

        public Task<T> RunOrchestration<T>(OrchestrationInstance instance, Type orchestrationType, object input)
        {
            return RunOrchestration<T>(instance, NameVersionHelper.GetDefaultName(orchestrationType),
                NameVersionHelper.GetDefaultVersion(orchestrationType), input);
        }

        public Task<T> RunOrchestration<T>(string name, string version, object input)
        {
            var instance = new OrchestrationInstance {InstanceId = "Test"};
            return RunOrchestration<T>(instance, name, version, input);
        }

        public async Task<T> RunOrchestration<T>(OrchestrationInstance instance, string name, string version,
            object input)
        {
            if (instance == null || string.IsNullOrWhiteSpace(instance.InstanceId))
            {
                throw new ArgumentException("instance");
            }

            Task<T> result = orchestrationExecutor.ExecuteOrchestration<T>(instance, taskActivityExecutor, clock,
                name, version, input);

            TimeSpan nextDelay;
            while (!(result.IsCompleted || result.IsFaulted || result.IsCanceled)
                   || taskActivityExecutor.HasPendingExecutions
                   || clock.HasPendingTimers)
            {
                nextDelay = clock.FirePendingTimers();
                if (nextDelay.Equals(TimeSpan.Zero))
                {
                    await Task.Delay(10);
                }
                else
                {
                    TimeSpan timeToSleep = TimeSpan.FromTicks(nextDelay.Ticks/ClockSpeedUpFactor);
                    try
                    {
                        WaitingForTimerToFire = true;
                        await Task.Delay(timeToSleep);
                    }
                    finally
                    {
                        WaitingForTimerToFire = false;
                    }
                    clock.MoveClockForward(nextDelay);
                }
            }

            try
            {
                return await result;
            }
            catch (SubOrchestrationFailedException e)
            {
                if (e.InnerException != null)
                {
                    throw e.InnerException;
                }

                throw e;
            }
        }

        public void RaiseEvent(OrchestrationInstance orchestrationInstance, string eventName, object eventData)
        {
            orchestrationExecutor.RaiseEvent(orchestrationInstance, eventName, eventData);
        }

        public T GetStatus<T>(OrchestrationInstance orchestrationInstance)
        {
            return orchestrationExecutor.GetStatus<T>(orchestrationInstance);
        }

        public async Task Delay(long milliseconds)
        {
            await
                clock.CreateTimer<object>(clock.CurrentUtcDateTime.AddMilliseconds(milliseconds), null,
                    CancellationToken.None);
        }
    }
}