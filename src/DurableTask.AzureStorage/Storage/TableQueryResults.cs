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
namespace DurableTask.AzureStorage.Storage
{
    using System;
    using System.Collections.Generic;

    sealed class TableQueryResults<T>
    {
        public TableQueryResults(IReadOnlyList<T> entities, TimeSpan elapsed, int requestCount)
        {
            this.Entities = entities ?? throw new ArgumentNullException(nameof(entities));
            this.Elapsed = elapsed;
            this.RequestCount = requestCount;
        }

        public IReadOnlyList<T> Entities { get; }

        public TimeSpan Elapsed { get; }

        public int ElapsedMilliseconds => (int)this.Elapsed.TotalMilliseconds;

        public int RequestCount { get; }
    }
}
