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

namespace DurableTask.AzureStorage.Tests.Correlation
{
    using System;

    public static class StringExtensions
    {
        public static TraceParent ToTraceParent(this string traceParent)
        {
            if (!string.IsNullOrEmpty(traceParent))
            {
                var substrings = traceParent.Split('-');
                if (substrings.Length != 4)
                {
                    throw new ArgumentException($"Traceparent doesn't respect the spec. {traceParent}");
                }

                return new TraceParent
                {
                    Version = substrings[0],
                    TraceId = substrings[1],
                    SpanId = substrings[2],
                    TraceFlags = substrings[3]
                };
            }

            return null;
        }
    }

}
