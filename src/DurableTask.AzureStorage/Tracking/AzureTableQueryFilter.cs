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
        /// Builds a <c>PartitionKey eq '...'</c> condition for an orchestration instance ID. The instance ID
        /// is <b>unsanitized</b>: this method applies <see cref="KeySanitation.EscapePartitionKey"/> itself and
        /// then OData quote-escaping. Do not pre-sanitize the value, because
        /// <see cref="KeySanitation.EscapePartitionKey"/> is not idempotent. Contrast with
        /// <see cref="PartitionKeyGreaterOrEqual"/> / <see cref="PartitionKeyLessThan"/>, which take an
        /// already-sanitized partition key.
        /// </summary>
        public static string PartitionKeyEquals(string unsanitizedInstanceId)
        {
            string sanitizedInstanceId = KeySanitation.EscapePartitionKey(unsanitizedInstanceId);
            return TableClient.CreateQueryFilter($"PartitionKey eq {sanitizedInstanceId}");
        }

        /// <summary>
        /// Builds a <c>{columnName} eq '...'</c> condition with <paramref name="rawValue"/> OData quote-escaped.
        /// This does <b>not</b> apply <see cref="KeySanitation.EscapePartitionKey"/>: it is for
        /// non-partition-key columns (e.g. ExecutionId, EventType, RowKey), so <paramref name="rawValue"/> is
        /// used as-is aside from OData escaping. The <paramref name="columnName"/> must be a trusted constant
        /// (never user input), since it is emitted as literal filter text rather than an escaped value.
        /// </summary>
        public static string ColumnEquals(string columnName, string rawValue)
        {
            // CreateQueryFilter takes a FormattableString and escapes/quotes each interpolation hole as a
            // value, while leaving the literal text of the format untouched. The column name must remain
            // literal (an interpolated {columnName} would be emitted as a quoted value, e.g. 'ExecutionId'
            // eq 'x', which is invalid), but it is a runtime parameter rather than a compile-time literal,
            // so a normal interpolated string ($"...") can't express that. FormattableStringFactory lets us
            // bake the column name into the format text while keeping the value as the only escaped argument.
            return TableClient.CreateQueryFilter(FormattableStringFactory.Create(columnName + " eq {0}", rawValue));
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
