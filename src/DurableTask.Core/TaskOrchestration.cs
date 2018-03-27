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

namespace DurableTask.Core
{
    using System;
    using System.Threading.Tasks;
    using DurableTask.Core.Common;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.Serializing;

    /// <summary>
    /// Base class for TaskOrchestration
    ///     User activity should almost always derive from either 
    ///     TaskOrchestration&lt;TResult, TInput&gt; or 
    ///     TaskOrchestration&lt;TResult, TInput, TEvent, TStatus&gt;
    /// </summary>
    public abstract class TaskOrchestration
    {
        /// <summary>
        /// Abstract method for executing an orchestration based on the context and serialized input
        /// </summary>
        /// <param name="context">The orchestration context</param>
        /// <param name="input">The serialized input</param>
        /// <returns>Serialized output from the execution</returns>
        public abstract Task<string> Execute(OrchestrationContext context, string input);

        /// <summary>
        /// Abstract method for raising an event in the orchestration
        /// </summary>
        /// <param name="context">The orchestration context</param>
        /// <param name="name">Name for this event to be passed to the onevent handler</param>
        /// <param name="input">The serialized input</param>
        public abstract void RaiseEvent(OrchestrationContext context, string name, string input);

        /// <summary>
        /// Gets the current status of the orchestration
        /// </summary>
        /// <returns>The status</returns>
        public abstract string GetStatus();
    }

    /// <summary>
    /// Typed base class for task orchestration
    /// </summary>
    /// <typeparam name="TResult">Output type of the orchestration</typeparam>
    /// <typeparam name="TInput">Input type for the orchestration</typeparam>
    public abstract class TaskOrchestration<TResult, TInput> : TaskOrchestration<TResult, TInput, string, string>
    {
    }

    /// <summary>
    /// Typed base class for Task orchestration with typed events and status
    /// </summary>
    /// <typeparam name="TResult">Output type of the orchestration</typeparam>
    /// <typeparam name="TInput">Input type for the orchestration</typeparam>
    /// <typeparam name="TEvent">Input type for RaiseEvent calls</typeparam>
    /// <typeparam name="TStatus">Output Type for GetStatus calls</typeparam>
    public abstract class TaskOrchestration<TResult, TInput, TEvent, TStatus> : TaskOrchestration
    {
        /// <summary>
        /// Creates a new TaskOrchestration with the default dataconverter
        /// </summary>
        public TaskOrchestration()
        {
            DataConverter = new JsonDataConverter();
        }

        /// <summary>
        /// The dataconverter to use for input and output serialization/deserialization
        /// </summary>
        public DataConverter DataConverter { get; protected set; }

        /// <summary>
        /// Method for executing an orchestration based on the context and serialized input
        /// </summary>
        /// <param name="context">The orchestration context</param>
        /// <param name="input">The serialized input</param>
        /// <returns>Serialized output from the execution</returns>
        public override async Task<string> Execute(OrchestrationContext context, string input)
        {
            var parameter = DataConverter.Deserialize<TInput>(input);
            TResult result = default(TResult);
            try
            {
                result = await RunTask(context, parameter);
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                string details = Utils.SerializeCause(e, DataConverter);
                throw new OrchestrationFailureException(e.Message, details);
            }

            return DataConverter.Serialize(result);
        }

        /// <summary>
        /// Method for raising an event in the orchestration
        /// </summary>
        /// <param name="context">The orchestration context</param>
        /// <param name="name">Name for this event to be passed to the onevent handler</param>
        /// <param name="input">The serialized input</param>
        public override void RaiseEvent(OrchestrationContext context, string name, string input)
        {
            var parameter = DataConverter.Deserialize<TEvent>(input);
            OnEvent(context, name, parameter);
        }

        /// <summary>
        /// Gets the current status of the orchestration
        /// </summary>
        /// <returns>The string status</returns>
        public override string GetStatus()
        {
            TStatus status = OnGetStatus();
            return DataConverter.Serialize(status);
        }

        /// <summary>
        /// Method for executing the orchestration with context and typed input
        /// </summary>
        /// <param name="context">The orchestraion context</param>
        /// <param name="input">The typed input</param>
        /// <returns>The typed output</returns>
        public abstract Task<TResult> RunTask(OrchestrationContext context, TInput input);

        /// <summary>
        /// Virtual method for processing an event with given context, name and typed input
        /// </summary>
        /// <param name="context">The orchestraion context</param>
        /// <param name="name">Name for this event</param>
        /// <param name="input">Typed input</param>
        public virtual void OnEvent(OrchestrationContext context, string name, TEvent input)
        {
            // do nothing
        }

        /// <summary>
        /// Method for getting typed status of the orchestration
        /// </summary>
        /// <returns>The typed status</returns>
        public virtual TStatus OnGetStatus()
        {
            return default(TStatus);
        }
    }
}