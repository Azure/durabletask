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
    using System;

    /// <summary>
    /// Extension methods for DateTime
    /// </summary>
    public static class DateTimeUtils
    {
        /// <summary>
        /// Returns bool indicating is the datetime has a value set
        /// </summary>        
        public static bool IsSet(this DateTime dateTime)
        {
            return !(dateTime == DateTime.MinValue || dateTime == MinDateTime);
        }

        /// <summary>
        /// Returns minimum allowable DateTime, allows overriding this for the storage emulator.
        /// The Storage emulator supports a min datetime or DateTime.FromFileTimeUtc(0)
        /// Do not alter this value. Kept as field to have backward compatibility(#319).
        /// </summary>  
        public static DateTime MinDateTime = DateTime.MinValue;

        /// <summary>
        /// Uses reflection to alter the static readonly MinDateTime value for tests
        /// </summary>  
        public static void SetMinDateTimeForStorageEmulator()
        {
            MinDateTime = DateTime.FromFileTimeUtc(0);
        }
    }
}
