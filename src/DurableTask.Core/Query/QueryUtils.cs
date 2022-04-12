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

using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace DurableTask.Core.Query
{
    /// <summary>
    /// Utils class used in querying instance status
    /// </summary>
    public class QueryUtils
    {
        private static readonly JValue NullJValue = JValue.CreateNull();

        /// <summary>
        /// Method used to convert OrchestrationState to DurableOrchestrationStatus
        /// </summary>
        public static DurableOrchestrationStatus ConvertOrchestrationStateToStatus(OrchestrationState orchestrationState, JArray historyArray = null)
        {
            return new DurableOrchestrationStatus
            {
                Name = orchestrationState.Name,
                InstanceId = orchestrationState.OrchestrationInstance.InstanceId,
                CreatedTime = orchestrationState.CreatedTime,
                LastUpdatedTime = orchestrationState.LastUpdatedTime,
                RuntimeStatus = (OrchestrationRuntimeStatus)orchestrationState.OrchestrationStatus,
                CustomStatus = ParseToJToken(orchestrationState.Status),
                Input = ParseToJToken(orchestrationState.Input),
                Output = ParseToJToken(orchestrationState.Output),
                History = historyArray,
            };
        }

        internal static JToken ParseToJToken(string value)
        {
            if (value == null)
            {
                return NullJValue;
            }

            // Ignore whitespace
            value = value.Trim();
            if (value.Length == 0)
            {
                return string.Empty;
            }

            try
            {
                return ConvertToJToken(value);
            }
            catch (JsonReaderException)
            {
                // Return the raw string value as the fallback. This is common in terminate scenarios.
                return value;
            }
        }

        internal static JToken ConvertToJToken(string input)
        {
            JToken token = null;
            if (input != null)
            {
                using (var stringReader = new StringReader(input))
                using (var jsonTextReader = new JsonTextReader(stringReader) { DateParseHandling = DateParseHandling.None })
                {
                    return token = JToken.Load(jsonTextReader);
                }
            }

            return token;
        }
    }
}
