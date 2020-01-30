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

namespace DurableTask.AzureStorage
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    /// <summary>
    /// Extension methods for String
    /// </summary>
    public static class StringExtensions
    {
        /// <summary>
        /// Get the ClassName part delimited by +
        /// e.g. DurableTask.AzureStorage.Tests.Correlation.CorrelationScenarioTest+SayHelloActivity
        /// should be "SayHelloActivity"
        /// </summary>
        /// <param name="s"></param>
        public static string GetTargetClassName(this string s)
        {
            var index = s.IndexOf('+');
            return s.Substring(index + 1, s.Length - index - 1);
        }
    }
}
