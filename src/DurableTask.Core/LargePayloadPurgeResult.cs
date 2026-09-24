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

#nullable enable
namespace DurableTask.Core
{
    using System;

    /// <summary>
    /// The worker's purge outcome for one tombstone.
    /// </summary>
    public sealed class LargePayloadPurgeResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="LargePayloadPurgeResult"/> class.
        /// </summary>
        /// <param name="tombstoneToken">The opaque correlation token echoed from the tombstone.</param>
        /// <param name="disposition">The worker's classification of the purge outcome.</param>
        public LargePayloadPurgeResult(string tombstoneToken, LargePayloadPurgeDisposition disposition)
        {
            this.TombstoneToken = tombstoneToken ?? throw new ArgumentNullException(nameof(tombstoneToken));
            this.Disposition = disposition;
        }

        /// <summary>
        /// Gets the unchanged tombstone correlation token.
        /// </summary>
        public string TombstoneToken { get; }

        /// <summary>
        /// Gets the purge outcome. Validation and retry scheduling belong to the backing service.
        /// </summary>
        public LargePayloadPurgeDisposition Disposition { get; }
    }
}
