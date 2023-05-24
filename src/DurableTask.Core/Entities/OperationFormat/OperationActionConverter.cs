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
namespace DurableTask.Core.Entities.OperationFormat
{
    using System;
    using Newtonsoft.Json.Linq;
    using DurableTask.Core.Serializing;

    internal class OperationActionConverter : JsonCreationConverter<OperationAction>
    {
        protected override OperationAction CreateObject(Type objectType, JObject jObject)
        {
            if (jObject.TryGetValue("OperationActionType", StringComparison.OrdinalIgnoreCase, out JToken actionType))
            {
                var type = (OperationActionType)int.Parse((string)actionType);
                switch (type)
                {
                    case OperationActionType.SendSignal:
                        return new SendSignalOperationAction();
                    case OperationActionType.StartNewOrchestration:
                        return new StartNewOrchestrationOperationAction();
                    default:
                        throw new NotSupportedException("Unrecognized action type.");
                }
            }

            throw new NotSupportedException("Action Type not provided.");
        }
    }
}