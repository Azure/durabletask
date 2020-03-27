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
    using System.Threading.Tasks;
    using DurableTask.Core;
    using NCrontab;

    public class CronOrchestration : TaskOrchestration<string, string>
    {
        public override async Task<string> RunTask(OrchestrationContext context, string cronSchedule)
        {
            var numberOfTimes = 4;
            var runAfterEverySeconds = 10;

            for (var i = 1; i <= numberOfTimes; i++)
            {
                Console.WriteLine($"Schedule CronTask({i}) start");

                DateTime currentTime = context.CurrentUtcDateTime;
                DateTime fireAt;
                if (string.IsNullOrWhiteSpace(cronSchedule))
                {
                    fireAt = currentTime.AddSeconds(runAfterEverySeconds);
                }
                else
                {
                    CrontabSchedule schedule = CrontabSchedule.Parse(cronSchedule);
                    fireAt = schedule.GetNextOccurrence(context.CurrentUtcDateTime);
                }
                
                string attempt = await context.CreateTimer(fireAt, i.ToString());
                Console.WriteLine($"Schedule CronTask({i}) at {fireAt}");

                Task<string> resultTask = context.ScheduleTask<string>(typeof(CronTask), attempt);
                await resultTask;
            }

            return "Done";
        }
    }
}