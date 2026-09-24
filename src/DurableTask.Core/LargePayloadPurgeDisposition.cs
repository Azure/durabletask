// Copyright (c) .NET Foundation. All rights reserved.
// Relocated from Azure/azure-functions-durable-extension under the MIT License.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE

namespace DurableTask.Core
{
    /// <summary>
    /// The outcome of a worker's attempt to purge a tombstoned large payload.
    /// </summary>
    public enum LargePayloadPurgeDisposition
    {
        /// <summary>
        /// No outcome was specified. The backing service must reject this value, not treat it as success.
        /// </summary>
        Unspecified = 0,

        /// <summary>
        /// Terminal success: the payload was deleted, was already absent, or is not owned by the payload store.
        /// </summary>
        Deleted = 1,

        /// <summary>
        /// A potentially transient failure. The backing service schedules another attempt.
        /// </summary>
        Retry = 2,

        /// <summary>
        /// A deterministic failure. Preserve the tombstone for operator action without retrying it.
        /// </summary>
        Quarantined = 3,
    }
}
