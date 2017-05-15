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

namespace DurableTask.Core.Test
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    internal class FakeOrchestrationContext : OrchestrationContext
    {
        readonly TaskScheduler scheduler;

        public FakeOrchestrationContext(TaskScheduler scheduler, OrchestrationInstance instance,
            FakeTaskActivityExecutor taskActivityExecutor,
            FakeOrchestrationExecutor orchestrationExecutor, FakeOrchestrationClock clock)
        {
            IsReplaying = false;
            this.scheduler = scheduler;
            OrchestrationInstance = instance;
            TaskActivityExecutor = taskActivityExecutor;
            OrchestrationExecutor = orchestrationExecutor;
            Clock = clock;
        }

        public FakeTaskActivityExecutor TaskActivityExecutor { get; private set; }
        public FakeOrchestrationExecutor OrchestrationExecutor { get; private set; }
        public FakeOrchestrationClock Clock { get; private set; }
        public bool StartNew { get; private set; }
        public string NewVersion { get; private set; }
        public object ContinueAsNewInput { get; private set; }

        public override DateTime CurrentUtcDateTime
        {
            get { return Clock.CurrentUtcDateTime; }
            internal set { Clock.CurrentUtcDateTime = value; }
        }

        public override Task<TResult> ScheduleTask<TResult>(string name, string version, params object[] parameters)
        {
            return TaskActivityExecutor.ExecuteTask<TResult>(name, version, parameters);
        }

        public override Task<T> CreateTimer<T>(DateTime fireAt, T state)
        {
            return CreateTimer(fireAt, state, CancellationToken.None);
        }

        public override Task<T> CreateTimer<T>(DateTime fireAt, T state, CancellationToken cancelToken)
        {
            return Clock.CreateTimer(fireAt, state, cancelToken);
        }

        public override Task<T> CreateSubOrchestrationInstance<T>(string name, string version, string instanceId,
            object input)
        {
            var instance = new OrchestrationInstance {InstanceId = instanceId};
            return OrchestrationExecutor.ExecuteOrchestration<T>(instance, this, name, version, input);
        }

        public override Task<T> CreateSubOrchestrationInstance<T>(string name, string version, string instanceId,
            object input, IDictionary<string, string> tags)
        {
            var instance = new OrchestrationInstance { InstanceId = instanceId };
            // This does not support tags, it only accepts them and ignores them
            return OrchestrationExecutor.ExecuteOrchestration<T>(instance, this, name, version, input);
        }

        public override Task<T> CreateSubOrchestrationInstance<T>(string name, string version, object input)
        {
            var instance = new OrchestrationInstance {InstanceId = Guid.NewGuid().ToString("N")};
            return OrchestrationExecutor.ExecuteOrchestration<T>(instance, this, name, version, input);
        }

        public override void ContinueAsNew(object input)
        {
            StartNew = true;
            ContinueAsNewInput = input;
        }

        public override void ContinueAsNew(string newVersion, object input)
        {
            StartNew = true;
            ContinueAsNewInput = input;
            NewVersion = newVersion;
        }
    }
}