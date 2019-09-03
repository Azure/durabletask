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
    using System.Web;

    using DurableTask.AzureServiceFabric.Exceptions;

    /// <summary>
    /// Defines extensions methods.
    /// </summary>
    public static class Extensions
    {
        /// <summary>
        /// Validates given instanceId.
        /// </summary>
        /// <param name="instanceId">Id of an orchestration instance</param>
        /// <returns>boolean indicating whether instanceId is valid or not</returns>
        public static bool IsValidInstanceId(this string instanceId)
        {
            if (instanceId == null)
            {
                return false;
            }

            // InsanceId consists of valid url characters is treated as valid.
            var encodedInstanceId = HttpUtility.UrlEncode(instanceId);

            return instanceId.Equals(encodedInstanceId, StringComparison.OrdinalIgnoreCase);
        }

        internal static void EnsureValidInstanceId(this string instanceId)
        {
            if (!instanceId.IsValidInstanceId())
            {
                throw new InvalidInstanceIdException(instanceId);
            }
        }
    }
}
