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

using System;
using System.Threading;
using DurableTask.Core;

namespace Stateful1.Samples.Cron
{
    public sealed class CronTask : TaskActivity<string,string>
    {
        protected override string Execute(DurableTask.Core.TaskContext context, string input)
        {
            ServiceEventSource.Current.Message($"Executing Cron Job.  Started At: '{DateTime.Now}' Number: {input}");

            Thread.Sleep(2 * 1000);

            string completed = $"Cron Job '{input}' Completed...";
            ServiceEventSource.Current.Message(completed);

            return completed;
        }
    }
}
