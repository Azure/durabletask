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
namespace DurableTask.Core.Common
{
    using System.Text;

    /// <summary>
    /// Fast, non-cryptographic hash function helper.
    /// </summary>
    /// <remarks>
    /// See https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function.
    /// Tested with production data and random guids. The result was good distribution.
    /// </remarks>
    internal static class Fnv1aHashHelper
    {
        const uint FnvPrime = unchecked(16777619);
        const uint FnvOffsetBasis = unchecked(2166136261);

        /// <summary>
        /// Compute a hash for a given string.
        /// </summary>
        /// <param name="value">The string to hash.</param>
        /// <returns>a four-byte hash</returns>
        public static uint ComputeHash(string value)
        {
            return ComputeHash(value, encoding: null);
        }

        /// <summary>
        /// Compute a hash for a given string and encoding.
        /// </summary>
        /// <param name="value">The string to hash.</param>
        /// <param name="encoding">The encoding.</param>
        /// <returns>a four-byte hash</returns>
        public static uint ComputeHash(string value, Encoding encoding)
        {
            return ComputeHash(value, encoding, hash: FnvOffsetBasis);
        }

        /// <summary>
        /// Compute a hash for a given string, encoding, and hash modifier.
        /// </summary>
        /// <param name="value">The string to hash.</param>
        /// <param name="encoding">The encoding.</param>
        /// <param name="hash">The modifier hash.</param>
        /// <returns>a four-byte hash</returns>
        public static uint ComputeHash(string value, Encoding encoding, uint hash)
        {
            byte[] bytes = (encoding ?? Encoding.UTF8).GetBytes(value);
            return ComputeHash(bytes, hash);
        }

        /// <summary>
        /// Compute a hash for the given byte array.
        /// </summary>
        /// <param name="array">The byte array to hash.</param>
        /// <returns>a four-byte hash</returns>
        public static uint ComputeHash(byte[] array)
        {
            return ComputeHash(array, hash: FnvOffsetBasis);
        }

        /// <summary>
        /// Compute a hash for the given byte array.
        /// </summary>
        /// <param name="array">The byte array to hash.</param>
        /// <param name="hash">The modifier hash.</param>
        /// <returns>a four-byte hash</returns>
        public static uint ComputeHash(byte[] array, uint hash)
        {
            for (var i = 0; i < array.Length; i++)
            {
                unchecked
                {
                    hash ^= array[i];
                    hash *= FnvPrime;
                }
            }

            return hash;
        }
    }
}
