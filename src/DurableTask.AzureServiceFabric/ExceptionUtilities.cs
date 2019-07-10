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

namespace DurableTask.AzureServiceFabric
{
    using System;
    using System.Fabric;

    using DurableTask.AzureServiceFabric.Tracing;

    static class ExceptionUtilities
    {
        public static bool IsRetryableFabricException(Exception e)
        {
            return e is TimeoutException || e is FabricTransientException;
        }

        public static void LogReliableCollectionException(string uniqueIdentifier, int attemptNumber, Exception e, bool isTransient)
        {
            if (isTransient)
            {
                ServiceFabricProviderEventSource.Tracing.RetryableFabricException(uniqueIdentifier, attemptNumber, e.ToString());
            }
            else
            {
                ServiceFabricProviderEventSource.Tracing.ExceptionInReliableCollectionOperations(uniqueIdentifier, e.ToString());
            }
        }
    }
}
