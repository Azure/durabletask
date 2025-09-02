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

    /// <summary>
    /// Interface for providing custom properties from exceptions that will be included in FailureDetails.
    /// This interface is intended for implementation by the durabletask-dotnet layer, which will
    /// convert customer implementations to this interface and register them with DurableTask.Core.
    /// </summary>
    public interface IExceptionPropertiesProvider
    {
        /// <summary>
        /// Extracts custom properties from an exception.
        /// </summary>
        /// <param name="exception">The exception to extract properties from.</param>
        /// <returns>A dictionary of custom properties to include in the FailureDetails, or null if no properties should be added.</returns>
        IDictionary<string, object>? GetExceptionProperties(Exception exception);
    }
}
