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
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.Serialization;
    using DurableTask.Core.Exceptions;
    using Newtonsoft.Json;

    // NOTE: This class is very similar to https://github.com/microsoft/durabletask-dotnet/blob/main/src/Abstractions/TaskFailureDetails.cs.
    //       Any functional changes to this class should be mirrored in that class and vice versa.

    /// <summary>
    /// Details of an activity, orchestration, or entity operation failure.
    /// </summary>
    [Serializable]
    public class FailureDetails : IEquatable<FailureDetails>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="FailureDetails"/> class.
        /// </summary>
        /// <param name="errorType">The name of the error, which is expected to the the namespace-qualified name of the exception type.</param>
        /// <param name="errorMessage">The message associated with the error, which is expected to be the exception's <see cref="Exception.Message"/> property.</param>
        /// <param name="stackTrace">The exception stack trace.</param>
        /// <param name="innerFailure">The inner cause of the failure.</param>
        /// <param name="isNonRetriable">Whether the failure is non-retriable.</param>
        [JsonConstructor]
        public FailureDetails(string errorType, string errorMessage, string? stackTrace, FailureDetails? innerFailure, bool isNonRetriable)
        {
            this.ErrorType = errorType;
            this.ErrorMessage = errorMessage;
            this.StackTrace = stackTrace;
            this.InnerFailure = innerFailure;
            this.IsNonRetriable = isNonRetriable;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FailureDetails"/> class from an exception object.
        /// </summary>
        /// <param name="e">The exception used to generate the failure details.</param>
        /// <param name="innerFailure">The inner cause of the failure.</param>
        public FailureDetails(Exception e, FailureDetails innerFailure)
            : this(e.GetType().FullName, GetErrorMessage(e), e.StackTrace, innerFailure, false)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FailureDetails"/> class from an exception object.
        /// </summary>
        /// <param name="e">The exception used to generate the failure details.</param>
        public FailureDetails(Exception e)
            : this(e.GetType().FullName, GetErrorMessage(e), e.StackTrace, FromException(e.InnerException), false)
        {
        }

        /// <summary>
        /// For testing purposes only: Initializes a new, empty instance of the <see cref="FailureDetails"/> class.
        /// </summary>
        public FailureDetails()
        {
            this.ErrorType = "None";
            this.ErrorMessage = string.Empty;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FailureDetails"/> class from a serialization context.
        /// </summary>
        protected FailureDetails(SerializationInfo info, StreamingContext context)
        {
            this.ErrorType = info.GetString(nameof(this.ErrorType));
            this.ErrorMessage = info.GetString(nameof(this.ErrorMessage));
            this.StackTrace = info.GetString(nameof(this.StackTrace));
            this.InnerFailure = (FailureDetails)info.GetValue(nameof(this.InnerFailure), typeof(FailureDetails));
        }

        /// <summary>
        /// Gets the type of the error, which is expected to the exception type's <see cref="Type.FullName"/> value.
        /// </summary>
        public string ErrorType { get; }

        /// <summary>
        /// Gets the message associated with the error, which is expected to be the exception's <see cref="Exception.Message"/> property.
        /// </summary>
        public string ErrorMessage { get; }

        /// <summary>
        /// Gets the exception stack trace.
        /// </summary>
        public string? StackTrace { get; }

        /// <summary>
        /// Gets the inner cause of this failure.
        /// </summary>
        public FailureDetails? InnerFailure { get; }

        /// <summary>
        /// Gets a value indicating whether this failure is non-retriable, meaning it should not be retried.
        /// </summary>
        public bool IsNonRetriable { get; }

        /// <summary>
        /// Gets a debug-friendly description of the failure information.
        /// </summary>
        public override string ToString()
        {
            return $"{this.ErrorType}: {this.ErrorMessage}";
        }

        /// <summary>
        /// Returns <c>true</c> if the task failure was provided by the specified exception type.
        /// </summary>
        /// <remarks>
        /// This method allows checking if a task failed due to an exception of a specific type by attempting
        /// to load the type specified in <see cref="ErrorType"/>. If the exception type cannot be loaded
        /// for any reason, this method will return <c>false</c>. Base types are supported.
        /// </remarks>
        /// <typeparam name="T">The type of exception to test against.</typeparam>
        /// <returns>Returns <c>true</c> if the <see cref="ErrorType"/> value matches <typeparamref name="T"/>; <c>false</c> otherwise.</returns>
        public bool IsCausedBy<T>() where T : Exception
        {
            // This check works for .NET exception types defined in System.Core.PrivateLib (aka mscorelib.dll)
            Type? exceptionType = Type.GetType(this.ErrorType, throwOnError: false);

            // For exception types defined in the same assembly as the target exception type.
            exceptionType ??= typeof(T).Assembly.GetType(this.ErrorType, throwOnError: false);

            // For custom exception types defined in the app's assembly.
            exceptionType ??= Assembly.GetCallingAssembly().GetType(this.ErrorType, throwOnError: false);

            if (exceptionType == null)
            {
                // This last check works for exception types defined in any loaded assembly (e.g. NuGet packages, etc.).
                // This is a fallback that should rarely be needed except in obscure cases.
                List<Type> matchingExceptionTypes = AppDomain.CurrentDomain.GetAssemblies()
                    .Select(a => a.GetType(this.ErrorType, throwOnError: false))
                    .Where(t => t is not null)
                    .ToList();
                if (matchingExceptionTypes.Count == 1)
                {
                    exceptionType = matchingExceptionTypes[0];
                }
                else if (matchingExceptionTypes.Count > 1)
                {
                    throw new AmbiguousMatchException($"Multiple exception types with the name '{this.ErrorType}' were found.");
                }
            }

            return exceptionType != null && typeof(T).IsAssignableFrom(exceptionType);
        }

        /// <summary>
        /// Gets whether two <see cref="FailureDetails"/> objects are equivalent using value semantics.
        /// </summary>
        public override bool Equals(object other) => Equals(other as FailureDetails);

        /// <summary>
        /// Gets whether two <see cref="FailureDetails"/> objects are equivalent using value semantics.
        /// </summary>
        public bool Equals(FailureDetails? other)
        {
            if (ReferenceEquals(other, null))
            {
                return false;
            }

            return
                this.ErrorType == other.ErrorType &&
                this.ErrorMessage == other.ErrorMessage &&
                this.StackTrace == other.StackTrace &&
                this.InnerFailure == other.InnerFailure;
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return (ErrorType, ErrorMessage, StackTrace, InnerFailure).GetHashCode();
        }

        static string GetErrorMessage(Exception e)
        {
            if (e is TaskFailedException tfe)
            {
                return $"Task '{tfe.Name}' (#{tfe.ScheduleId}) failed with an unhandled exception: {tfe.Message}";
            }
            else
            {
                return e.Message;
            }
        }

        static FailureDetails? FromException(Exception? e)
        {
            return e == null ? null : new FailureDetails(e);
        }
    }
}
