//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace DurableTaskSamplesUnitTests
{
    using System;
    using System.Threading.Tasks;
    using DurableTask;
    using DurableTask.Test;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using DurableTaskSamples.Cron;

    [TestClass]
    public class CronTest
    {
        class MockCronTask : TaskActivity<string, string>
        {
            public static int Count = 0;

            public MockCronTask()
            {
            }

            protected override string Execute(DurableTask.TaskContext context, string input)
            {
                Console.WriteLine("Executing Cron Job.  Started At: '" + DateTime.Now.ToString() + "' Number: " + input);

                string completed = "Cron Job '" + input + "' Completed...";
                Console.WriteLine(completed);

                Count++;
                return completed;
            }
        }

        [TestMethod]
        public async Task CronBasic()
        {
            OrchestrationTestHost host = new OrchestrationTestHost();
            host.ClockSpeedUpFactor = 5;
            MockCronTask cronTask = new MockCronTask();
            host.AddTaskOrchestrations(typeof(CronOrchestration))
                .AddTaskActivities(new MockObjectCreator<TaskActivity>(typeof(CronTask).ToString(), string.Empty, () =>
                    {
                        return cronTask;
                    }));

            var result = await host.RunOrchestration<string>(typeof(CronOrchestration), null);

            Assert.AreEqual<string>("Done", result);
            Assert.AreEqual<double>(4, MockCronTask.Count);
        }
    }
}
