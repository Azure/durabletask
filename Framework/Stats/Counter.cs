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

namespace DurableTask.Stats
{
    /// <summary>
    /// Simple counter class
    /// </summary>
    public class Counter
    {
        /// <summary>
        /// Gets the current counter value
        /// </summary>
        public long Value { get; private set; } = 0;

        /// <summary>
        /// Increments the counter by 1
        /// </summary>
        public void Increment()
        {
            ++Value;
        }

        /// <summary>
        /// Increments the counter by the supplied value
        /// </summary>
        /// <param name="value">The value to increment the counter by</param>
        public void Increment(long value)
        {
            Value += value;
        }

        /// <summary>
        /// Returns a string that represents the Counter.
        /// </summary>
        public override string ToString()
        {
            return Value.ToString();
        }
    }
}
