//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation
//  Licensed under the Apache License, Version 2.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------
#if !NET462
#nullable enable
namespace DurableTask.Core.Tests
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using DurableTask.Core.Entities.OperationFormat;
    using DurableTask.Core.Tracing;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using DiagnosticsActivityStatusCode = System.Diagnostics.ActivityStatusCode;
    using TraceActivityStatusCode = DurableTask.Core.Tracing.ActivityStatusCode;

    [TestClass]
    public class TraceHelperTests
    {
        [TestMethod]
        public void EndActivitiesForEntityInvocationResetsSuccessfulStatus()
        {
            var activities = new List<Activity>
            {
                new Activity("entityOperation").Start()
            };
            activities[0].SetStatus(TraceActivityStatusCode.Error, "instrumented error");

            var results = new List<OperationResult>
            {
                new OperationResult()
            };

            TraceHelper.EndActivitiesForProcessingEntityInvocation(activities, results, batchFailureDetails: null);

            Assert.AreEqual(DiagnosticsActivityStatusCode.Ok, activities[0].Status);
        }

        [TestMethod]
        public void EndActivitiesForEntityInvocationMarksFailures()
        {
            var activities = new List<Activity>
            {
                new Activity("entityOperation").Start()
            };

            var failingResults = new List<OperationResult>
            {
                new OperationResult
                {
                    ErrorMessage = "entity failure"
                }
            };

            TraceHelper.EndActivitiesForProcessingEntityInvocation(activities, failingResults, batchFailureDetails: null);

            Assert.AreEqual(DiagnosticsActivityStatusCode.Error, activities[0].Status);
        }
    }
}
#endif
