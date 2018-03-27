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

namespace DurableTask.Samples.ErrorHandling
{
    using System;
    using DurableTask.Core;

    public sealed class GoodTask : TaskActivity<string, string>
    {
        public GoodTask()
        {
        }

        protected override string Execute(DurableTask.Core.TaskContext context, string input)
        {
            Console.WriteLine("GoodTask Executed...");

            return "GoodResult";
        }
    }

    public sealed class BadTask : TaskActivity<string, string>
    {
        public BadTask()
        {
        }

        protected override string Execute(DurableTask.Core.TaskContext context, string input)
        {
            Console.WriteLine("BadTask Executed...");

            throw new InvalidOperationException("BadTask failed.");
        }
    }

    public sealed class CleanupTask : TaskActivity<string, string>
    {
        public CleanupTask()
        {
        }

        protected override string Execute(DurableTask.Core.TaskContext context, string input)
        {
            Console.WriteLine("CleanupTask Executed...");

            return "CleanupResult";
        }
    }

}