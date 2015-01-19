namespace DurableTaskSamples.Cron
{
    using System;
    using System.Threading.Tasks;
    using DurableTask;
    using NCrontab;

    public class CronOrchestration : TaskOrchestration<string, string>
    {
        public override async Task<string> RunTask(OrchestrationContext context, string cronSchedule)
        {
            int numberOfTimes = 4;
            bool waitForCompletion = false;
            int runAfterEverySeconds = 10;

            for (int i = 1; i <= numberOfTimes; i++)
            {
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
                
                var attempt = await context.CreateTimer<string>(fireAt, i.ToString());

                Task<string> resultTask = context.ScheduleTask<string>(typeof(CronTask), attempt);

                if (waitForCompletion)
                {
                    await resultTask;
                }
            }

            return "Done";
        }
    }
}