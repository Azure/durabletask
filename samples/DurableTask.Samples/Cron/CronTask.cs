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

namespace DurableTask.Samples.Cron
{
    using System;
    using System.Threading;
    using DurableTask.Core;

    public sealed class CronTask : TaskActivity<string,string>
    {
        public CronTask()
        {
        }

        protected override string Execute(DurableTask.Core.TaskContext context, string input)
        {
            Console.WriteLine($"Executing Cron Job.  Started At: '{DateTime.Now}' Number: {input}");

            Thread.Sleep(2 * 1000);

            string completed = $"Cron Job '{input}' Completed...";
            Console.WriteLine(completed);

            return completed;
        }
    }

}
