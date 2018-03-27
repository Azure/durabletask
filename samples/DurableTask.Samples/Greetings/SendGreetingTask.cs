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

namespace DurableTask.Samples.Greetings
{
    using System;
    using System.Threading;
    using DurableTask.Core;

    public sealed class SendGreetingTask : TaskActivity<string, string>
    {
        public SendGreetingTask()
        {
        }

        protected override string Execute(DurableTask.Core.TaskContext context, string user)
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