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

namespace DurableTask
{
    using System;
    using System.Threading.Tasks;
    using DurableTask.Common;
    using DurableTask.Exceptions;
    using DurableTask.Serializing;

    public abstract class TaskOrchestration
    {
        public abstract Task<string> Execute(OrchestrationContext context, string input);
        public abstract void RaiseEvent(OrchestrationContext context, string name, string input);
        public abstract string GetStatus();
    }

    public abstract class TaskOrchestration<TResult, TInput> : TaskOrchestration<TResult, TInput, string, string>
    {
    }

    public abstract class TaskOrchestration<TResult, TInput, TEvent, TStatus> : TaskOrchestration
    {
        public TaskOrchestration()
        {
            DataConverter = new JsonDataConverter();
        }

        public DataConverter DataConverter { get; protected set; }

        public override async Task<string> Execute(OrchestrationContext context, string input)
        {
            var parameter = DataConverter.Deserialize<TInput>(input);
            TResult result = default(TResult);
            try
            {
                result = await RunTask(context, parameter);
            }
            catch (Exception e)
            {
                string details = Utils.SerializeCause(e, DataConverter);
                throw new OrchestrationFailureException(e.Message, details);
            }

            return DataConverter.Serialize(result);
        }

        public override void RaiseEvent(OrchestrationContext context, string name, string input)
        {
            var parameter = DataConverter.Deserialize<TEvent>(input);
            OnEvent(context, name, parameter);
        }

        public override string GetStatus()
        {
            TStatus status = OnGetStatus();
            return DataConverter.Serialize(status);
        }

        public abstract Task<TResult> RunTask(OrchestrationContext context, TInput input);

        public virtual void OnEvent(OrchestrationContext context, string name, TEvent input)
        {
            // do nothing
        }

        public virtual TStatus OnGetStatus()
        {
            return default(TStatus);
        }
    }
}