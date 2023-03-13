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
namespace DurableTask.Core.Entities.EventFormat
{
    using System;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
 
    internal class ResponseMessage
    {
        [JsonProperty(PropertyName = "result")]
        public string Result { get; set; }

        [JsonProperty(PropertyName = "exceptionType", DefaultValueHandling = DefaultValueHandling.Ignore)]
        public string ErrorMessage { get; set; }

        [JsonProperty(PropertyName = "failureDetails", DefaultValueHandling = DefaultValueHandling.Ignore)]
        public FailureDetails FailureDetails { get; set; }

        [JsonIgnore]
        public bool IsErrorResult => this.ErrorMessage != null;

        public override string ToString()
        {
            if (this.IsErrorResult)
            {
                return $"[ErrorResponse {this.Result}]";
            }
            else
            {
                return $"[Response {this.Result}]";
            }
        }
    }
}
