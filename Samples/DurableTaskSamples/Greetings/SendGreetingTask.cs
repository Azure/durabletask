namespace DurableTaskSamples.Greetings
{
    using System;
    using System.Threading;
    using DurableTask;

    public sealed class SendGreetingTask : TaskActivity<string, string>
    {
        public SendGreetingTask()
        {
        }

        protected override string Execute(DurableTask.TaskContext context, string user)
        {
            string message = null;
            if (!string.IsNullOrWhiteSpace(user) && user.Equals("TimedOut"))
            {
                message = "GetUser Timed out!!!";
                Console.WriteLine(message);
            }
            else
            {
                Console.WriteLine("Sending greetings to user: " + user + "...");

                Thread.Sleep(5 * 1000);

                message = "Greeting sent to " + user;
                Console.WriteLine(message);
            }

            return message;
        }
    }

}