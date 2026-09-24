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
    /// A tombstoned large payload to be purged by the worker.
    /// </summary>
    public sealed class LargePayloadPurgeTombstone
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="LargePayloadPurgeTombstone"/> class.
        /// </summary>
        /// <param name="tombstoneToken">The opaque backend-issued correlation token.</param>
        /// <param name="payloadToken">The unchanged external payload token.</param>
        public LargePayloadPurgeTombstone(string tombstoneToken, string payloadToken)
        {
            this.TombstoneToken = tombstoneToken ?? throw new ArgumentNullException(nameof(tombstoneToken));
            this.PayloadToken = payloadToken ?? throw new ArgumentNullException(nameof(payloadToken));
        }

        /// <summary>
        /// Gets the opaque correlation token that must be echoed unchanged in the purge result.
        /// </summary>
        public string TombstoneToken { get; }

        /// <summary>
        /// Gets the external payload token, which must not be interpreted or normalized by intermediaries.
        /// </summary>
        public string PayloadToken { get; }
    }
}
