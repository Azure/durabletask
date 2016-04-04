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

namespace DurableTask.Common
{
    using System;
    using System.Collections.Generic;

    public class CustomEqualityComparer<T> : IEqualityComparer<T>
    {
        Func<T, T, bool> comparer;

        public CustomEqualityComparer(Func<T, T, bool> comparer)
        {
            this.comparer = comparer;
        }

        public bool Equals(T objectA, T objectB)
        {
            return this.comparer(objectA, objectB);
        }

        public int GetHashCode(T obj)
        {
            return obj.GetHashCode();
        }
    }
}
