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

namespace Correlation.Samples
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using DurableTask.Core;
    using Microsoft.ApplicationInsights.DataContracts;

    public static class CorrelatedExceptionExtensions
    {
        public static ExceptionTelemetry CreateExceptionTelemetry(this CorrelatedException e)
        {
            var exceptionTelemetry = new ExceptionTelemetry(e.Exception);
            exceptionTelemetry.Context.Operation.Id = e.OperationId;
            exceptionTelemetry.Context.Operation.ParentId = e.ParentId;
            return exceptionTelemetry;
        }
    }
}
