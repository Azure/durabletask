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
    using System.Collections.Generic;
    using System.Linq;
    using DurableTask.History;
    using Newtonsoft.Json;

    public class DebugTestHost
    {
        NameVersionObjectManager<TaskOrchestration> orchestrationObjectManager;

        public DebugTestHost()
        {
            this.orchestrationObjectManager = new NameVersionObjectManager<TaskOrchestration>();
        }

        public DebugTestHost AddTaskOrchestrations(params Type[] taskOrchestrationTypes)
        {
            foreach (Type type in taskOrchestrationTypes)
            {
                ObjectCreator<TaskOrchestration> creator = new DefaultObjectCreator<TaskOrchestration>(type);
                this.orchestrationObjectManager.Add(creator);
            }

            return this;
        }

        public DebugTestHost AddTaskOrchestrations(params ObjectCreator<TaskOrchestration>[] taskOrchestrationCreators)
        {
            foreach (ObjectCreator<TaskOrchestration> creator in taskOrchestrationCreators)
            {
                this.orchestrationObjectManager.Add(creator);
            }

            return this;
        }

        public string ReplayOrchestration(Type orchestrationType, string serializedHistoryEvents)
        {
            return ReplayOrchestration(
                NameVersionHelper.GetDefaultName(orchestrationType),
                NameVersionHelper.GetDefaultVersion(orchestrationType),
                serializedHistoryEvents);
        }

        public string ReplayOrchestration(string name, string version, string serializedHistoryEvents)
        {

            TaskOrchestration taskOrchestration = this.orchestrationObjectManager.GetObject(name, version);

            IList<HistoryEvent> replayEvents = this.DeserializeHistoryEvents(serializedHistoryEvents);

            if (replayEvents.Any(re => re.EventType == EventType.GenericEvent))
            {
                throw new InvalidOperationException("Cannot replay with GenericEvent");
            }

            var runtimeState = new OrchestrationRuntimeState(this.DeserializeHistoryEvents(serializedHistoryEvents));

            TaskOrchestrationExecutor executor = new TaskOrchestrationExecutor(runtimeState, taskOrchestration);
            return JsonConvert.SerializeObject(executor.Execute(), new JsonSerializerSettings()
            {
                TypeNameHandling = TypeNameHandling.All,
                Formatting = Formatting.Indented
            });
        }

        IList<HistoryEvent> DeserializeHistoryEvents(string serializedHistoryEvents)
        {
            return JsonConvert.DeserializeObject<IList<HistoryEvent>>(
                serializedHistoryEvents,
                new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.All });
        }
    }
}
