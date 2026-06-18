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

namespace DurableTask.AzureStorage.Tracking
{
    using System.Runtime.CompilerServices;
    using Azure.Data.Tables;

    /// <summary>
    /// Builds individual OData filter conditions for Azure Table Storage queries with proper escaping
    /// to prevent OData injection via user-influenced values (e.g. instance IDs, execution IDs, task hub
    /// names). Each helper returns a single condition string; callers compose them with " and " / " or ".
    /// </summary>
    static class AzureTableQueryFilter
    {
        /// <summary>
        /// Builds a <c>PartitionKey eq '...'</c> condition for the given orchestration instance ID.
        /// Applies <see cref="KeySanitation.EscapePartitionKey"/> for storage-key sanitization and then
        /// OData quote-escaping. Pass the <b>raw</b> instance ID; do not pre-sanitize it, because
        /// <see cref="KeySanitation.EscapePartitionKey"/> is not idempotent.
        /// </summary>
        public static string PartitionKeyEquals(string instanceId)
        {
            string sanitizedInstanceId = KeySanitation.EscapePartitionKey(instanceId);
            return TableClient.CreateQueryFilter($"PartitionKey eq {sanitizedInstanceId}");
        }

        /// <summary>
        /// Builds a <c>{columnName} eq '...'</c> condition with <paramref name="value"/> OData-escaped.
        /// The <paramref name="columnName"/> must be a trusted constant (never user input), since it is
        /// emitted as literal filter text rather than an escaped value.
        /// </summary>
        public static string ColumnEquals(string columnName, string value)
        {
            return TableClient.CreateQueryFilter(FormattableStringFactory.Create(columnName + " eq {0}", value));
        }

        /// <summary>
        /// Builds a <c>PartitionKey ge '...'</c> condition. The value must already be sanitized via
        /// <see cref="KeySanitation.EscapePartitionKey"/> by the caller.
        /// </summary>
        public static string PartitionKeyGreaterOrEqual(string sanitizedPartitionKey)
        {
            return TableClient.CreateQueryFilter($"PartitionKey ge {sanitizedPartitionKey}");
        }

        /// <summary>
        /// Builds a <c>PartitionKey lt '...'</c> condition. The value must already be sanitized via
        /// <see cref="KeySanitation.EscapePartitionKey"/> by the caller.
        /// </summary>
        public static string PartitionKeyLessThan(string sanitizedPartitionKey)
        {
            return TableClient.CreateQueryFilter($"PartitionKey lt {sanitizedPartitionKey}");
        }
    }
}
