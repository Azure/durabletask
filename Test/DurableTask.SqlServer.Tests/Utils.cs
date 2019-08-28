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

namespace DurableTask.SqlServer.Tests
{
    using DurableTask.Core;
    using DurableTask.Core.History;
    using DurableTask.Core.Tracking;
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public static class Utils
    {
        private static readonly Random random = new Random();

        public static IEnumerable<OrchestrationStateInstanceEntity> InfiniteOrchestrationTestData()
        {
            var statusValues = Enum.GetValues(typeof(OrchestrationStatus)).Cast<OrchestrationStatus>().ToArray();

            while (true)
                yield return new OrchestrationStateInstanceEntity
                {
                    State = new OrchestrationState
                    {
                        CompletedTime = DateTime.MaxValue,
                        CreatedTime = DateTime.UtcNow,
                        Input = $"\"{GetRandomStringValue()}\"",
                        LastUpdatedTime = DateTime.UtcNow,
                        Name = GetRandomStringValue(),
                        OrchestrationInstance = new OrchestrationInstance
                        {
                            ExecutionId = Guid.NewGuid().ToString("N"),
                            InstanceId = Guid.NewGuid().ToString("N")
                        },
                        OrchestrationStatus = statusValues[random.Next(statusValues.Length)],
                        Version = string.Empty,
                    }
                };
        }

        public static IEnumerable<OrchestrationWorkItemInstanceEntity> InfiniteWorkItemTestData(string instanceId, string executionId)
        {
            var sequenceNumber = 0;
            while (true)
                yield return new OrchestrationWorkItemInstanceEntity
                {
                    EventTimestamp = DateTime.UtcNow,
                    ExecutionId = executionId,
                    InstanceId = instanceId,
                    SequenceNumber = sequenceNumber++,
                    HistoryEvent = new GenericEvent(random.Next(100), GetRandomStringValue())
                };
        }

        private static string GetRandomStringValue(int minimumLength = 5, int maximumLength = 15)
        {
            var length = random.Next(maximumLength - minimumLength) + minimumLength;

            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }
    }
}
