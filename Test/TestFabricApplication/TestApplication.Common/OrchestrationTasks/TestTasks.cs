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

namespace TestApplication.Common.OrchestrationTasks
{
    using System.Threading.Tasks;

    using TestApplication.Common.Orchestrations;

    public class TestTasks : ITestTasks
    {
        static int generationCount = 0;

        /// <summary>
        /// Increments Generation Count variable.
        /// </summary>
        /// <returns>Generation count</returns>
        public Task<int> IncrementGenerationCount()
        {
            return Task.FromResult(++generationCount);
        }

        /// <summary>
        /// Utility method to reset counter at the beginning of test.
        /// </summary>
        /// <returns>Generation coutner value</returns>
        public Task<int> ResetGenerationCounter()
        {
            generationCount = 0;
            return Task.FromResult(generationCount);
        }

        /// <summary>
        /// Throws exception when remainingAttempts > 0. Otherwise succeeds.
        /// </summary>
        /// <param name="remainingAttempts">remaining number of attempts</param>
        /// <returns>bool indicating whether task completed successfully or not.</returns>
        public  Task<bool> ThrowExceptionAsync(int remainingAttempts)
        {
            if (remainingAttempts > 0)
            {
                throw new CounterException(remainingAttempts);
            }

            return Task.FromResult(true);
        }
    }
}
