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

namespace DurableTask.Core.Command
{
    using System;
    using Newtonsoft.Json.Linq;
    using DurableTask.Core.Serializing;

    internal class OrchestrationActionConverter : JsonCreationConverter<OrchestratorAction>
    {
        protected override OrchestratorAction CreateObject(Type objectType, JObject jobject)
        {
            JToken actionType;
            if (jobject.TryGetValue("OrchestratorActionType", StringComparison.OrdinalIgnoreCase, out actionType))
            {
                var type = (OrchestratorActionType)int.Parse((string)actionType);
                switch (type)
                {
                    case OrchestratorActionType.CreateTimer:
                        return new CreateTimerOrchestratorAction();
                    case OrchestratorActionType.OrchestrationComplete:
                        return new OrchestrationCompleteOrchestratorAction();
                    case OrchestratorActionType.ScheduleOrchestrator:
                        return new ScheduleTaskOrchestratorAction();
                    case OrchestratorActionType.CreateSubOrchestration:
                        return new CreateSubOrchestrationAction();
                    default:
                        throw new NotSupportedException("Unrecognized action type.");
                }
            }
            throw new NotSupportedException("Action Type not provided.");
        }
    }
}