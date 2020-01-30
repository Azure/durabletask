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

namespace DurableTask.Core
{
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// Extension methods for Stack
    /// </summary>
    public static class StackExtensions
    {
        /// <summary>
        /// Clone the Stack instance with the right order.
        /// </summary>
        /// <typeparam name="T">Type of the Stack</typeparam>
        /// <param name="original">Stack instance</param>
        /// <returns></returns>
        public static Stack<T> Clone<T>(this Stack<T> original)
        {
            return new Stack<T>(original.Reverse());
        }
    }
}
