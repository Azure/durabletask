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

namespace DurableTask.AzureStorage.Partitioning
{
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Storage;
    using Newtonsoft.Json;

    class BlobPartitionLease : Lease
    {
        public BlobPartitionLease()
            : base()
        {
        }

        public BlobPartitionLease(Blob leaseBlob)
            : this()
        {
            this.Blob = leaseBlob;
        }

        public BlobPartitionLease(BlobPartitionLease source)
            : base(source)
        {
            this.Blob = source.Blob;
        }

        [JsonIgnore]
        public Blob Blob { get; set; }

        public override async Task<bool> IsExpiredAsync(CancellationToken cancellationToken = default)
        {
            return !await this.Blob.IsLeasedAsync(cancellationToken);
        }
    }
}
