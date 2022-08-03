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

namespace DurableTask.ServiceBus.Common
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Azure;

    internal sealed class AsyncPageableProjection<TSource, TResult> : AsyncPageable<TResult>
    {
        private readonly AsyncPageable<TSource> _source;
        private readonly Func<TSource, TResult> _selector;

        public AsyncPageableProjection(AsyncPageable<TSource> source, Func<TSource, TResult> selector)
        {
            _source = source ?? throw new ArgumentNullException(nameof(source));
            _selector = selector ?? throw new ArgumentNullException(nameof(selector));
        }

        public override IAsyncEnumerable<Page<TResult>> AsPages(string continuationToken = null, int? pageSizeHint = null)
        {
            return _source
                .AsPages(continuationToken, pageSizeHint)
                .Select(x => Page<TResult>.FromValues(
                    x.Values.Select(_selector).ToList(),
                    continuationToken,
                    x.GetRawResponse()));
        }
    }
}
