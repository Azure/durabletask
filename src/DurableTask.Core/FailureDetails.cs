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
#nullable enable
namespace DurableTask.Core
{
    using System;
    using DurableTask.Core.Exceptions;
    using Newtonsoft.Json;

    /// <summary>
    /// Details of an activity or orchestration failure.
    /// </summary>
    public class FailureDetails
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="FailureDetails"/> class.
        /// </summary>
        /// <param name="errorName">The name of the error, which is expected to the the namespace-qualified name of the exception type.</param>
        /// <param name="errorMessage">The message associated with the error, which is expected to be the exception's <see cref="Exception.Message"/> property.</param>
        /// <param name="errorDetails">The full details of the error, which is expected to be the output of <see cref="Exception.ToString"/>.</param>
        [JsonConstructor]
        public FailureDetails(string errorName, string errorMessage, string? errorDetails)
        {
            this.ErrorName = errorName;
            this.ErrorMessage = errorMessage;
            this.ErrorDetails = errorDetails;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FailureDetails"/> class from an exception object.
        /// </summary>
        /// <param name="e">The exception used to generate the failure details.</param>
        public FailureDetails(Exception e)
            : this(e.GetType().FullName, GetErrorMessage(e), e.ToString())
        {
        }

        /// <summary>
        /// Gets the name of the error, which is expected to the the namespace-qualified name of the exception type.
        /// </summary>
        public string ErrorName { get; }

        /// <summary>
        /// Gets the message associated with the error, which is expected to be the exception's <see cref="Exception.Message"/> property.
        /// </summary>
        public string ErrorMessage { get; }

        /// <summary>
        /// Gets the full details of the error, which is expected to be the output of <see cref="Exception.ToString"/>.
        /// </summary>
        public string? ErrorDetails { get; }

        /// <summary>
        /// Gets a debug-friendly description of the failure information.
        /// </summary>
        public override string ToString()
        {
            return $"{this.ErrorName}: {this.ErrorMessage}";
        }

        static string GetErrorMessage(Exception e)
        {
            if (e is TaskFailedException tfe)
            {
                return $"Task '{tfe.Name}' (#{tfe.ScheduleId}) failed with an unhandled exception: {tfe.InnerException?.Message}";
            }
            else
            {
                return e.Message;
            }
        }
    }
}
