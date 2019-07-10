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

namespace DurableTask.AzureServiceFabric.Service
{
    using System;
    using System.Security.Cryptography;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Implements <see cref="IPartitionHashing{T}"/> for string elements.
    /// </summary>
    public class DefaultStringPartitionHashing : IPartitionHashing<string>
    {
        /// <inheritdoc/>
        public Task<long> GeneratePartitionHashCodeAsync(string value, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            long hashCode = 0;
            if (!string.IsNullOrEmpty(value))
            {
                using (var sha256 = SHA256Managed.Create())
                {
                    var bytes = Encoding.UTF8.GetBytes(value);
                    var hash = sha256.ComputeHash(bytes);
                    var long1 = BitConverter.ToInt64(hash, 0);
                    var long2 = BitConverter.ToInt64(hash, 8);
                    var long3 = BitConverter.ToInt64(hash, 16);
                    var long4 = BitConverter.ToInt64(hash, 24);

                    hashCode = long1 ^ long2 ^ long3 ^ long4;
                }
            }

            return Task.FromResult(hashCode);
        }
    }
}
