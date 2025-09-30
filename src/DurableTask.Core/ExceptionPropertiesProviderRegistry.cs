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
    /// <summary>
    /// Registry for the global exception properties provider.
    /// This registry is intended for use by the durabletask-dotnet layer to set a provider
    /// that will be used when creating FailureDetails from exceptions.
    /// </summary>
    public static class ExceptionPropertiesProviderRegistry
    {
        /// <summary>
        /// Gets or sets the global exception properties provider.
        /// This will be set by the durabletask-dotnet layer.
        /// </summary>
        public static IExceptionPropertiesProvider? Provider { get; set; }
    }
}

