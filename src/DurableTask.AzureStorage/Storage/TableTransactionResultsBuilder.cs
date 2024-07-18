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
    using Azure;

    sealed class TableTransactionResultsBuilder
    {
        TimeSpan _elapsed;
        int _requestCount;
        List<Response> _responses = new List<Response>();

        public TableTransactionResultsBuilder Add(TableTransactionResults batch)
        {
            if (batch == null)
            {
                throw new ArgumentNullException(nameof(batch));
            }

            this._responses.AddRange(batch.Responses);
            this._elapsed += batch.Elapsed;
            this._requestCount += batch.RequestCount;

            return this;
        }

        public TableTransactionResults ToResults()
        {
            return new TableTransactionResults(this._responses, this._elapsed, this._requestCount);
        }
    }
}
