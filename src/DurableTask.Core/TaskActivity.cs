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
    using Newtonsoft.Json.Linq;

    /// <summary>
    ///     Base class for TaskActivity.
    ///     User activity should almost always derive from either TypedTaskActivity
    ///     &lt;TInput, TResult&gt; or TaskActivity&lt;TInput, TResult&gt;
    /// </summary>
    public abstract class TaskActivity
    {
        /// <summary>
        /// Abstract method for executing a task activity syncronously
        /// </summary>
        /// <param name="context">The task context</param>
        /// <param name="input">The serialized input</param>
        /// <returns>Serialized output from the execution</returns>
        public abstract string Run(TaskContext context, string input);

        /// <summary>
        /// Virtual method for executing a task activity asyncronously
        /// </summary>
        /// <param name="context">The task context</param>
        /// <param name="input">The serialized input</param>
        /// <returns>Serialized output from the execution</returns>
        public virtual Task<string> RunAsync(TaskContext context, string input)
        {
            return Task.FromResult(Run(context, input));
        }
    }

    /// <summary>
    ///     Typed base class for creating typed async task activities
    /// </summary>
    /// <typeparam name="TInput">Input type for the activity</typeparam>
    /// <typeparam name="TResult">Output type of the activity</typeparam>
    public abstract class AsyncTaskActivity<TInput, TResult> : TaskActivity
    {
        /// <summary>
        /// Creates a new AsyncTaskActivity with the default dataconverter
        /// </summary>
        protected AsyncTaskActivity()
        {
            DataConverter = new JsonDataConverter();
        }

        /// <summary>
        /// Creates a new AsyncTaskActivity with the supplied dataconverter
        /// </summary>
        /// <param name="dataConverter"></param>
        protected AsyncTaskActivity(DataConverter dataConverter)
        {
            if (dataConverter != null)
            {
                DataConverter = dataConverter;
            }
            else
            {
                DataConverter = new JsonDataConverter();
            }
        }

        /// <summary>
        /// The dataconverter to use for input and output serialization/deserialization
        /// </summary>
        public DataConverter DataConverter { get; protected set; }

        /// <summary>
        /// Syncronous execute method, blocked for AsyncTaskActivity
        /// </summary>
        /// <returns>string.Empty</returns>
        public override string Run(TaskContext context, string input)
        {
            // will never run
            return string.Empty;
        }

        /// <summary>
        /// Abstract method for executing a task activity asyncronously
        /// </summary>
        /// <param name="context">The task context</param>
        /// <param name="input">The typed input</param>
        /// <returns>The typed output from the execution</returns>
        protected abstract Task<TResult> ExecuteAsync(TaskContext context, TInput input);

        /// <summary>
        /// Method for executing a task activity asyncronously
        /// </summary>
        /// <param name="context">The task context</param>
        /// <param name="input">The serialized input</param>
        /// <returns>Serialized output from the execution</returns>
        public override async Task<string> RunAsync(TaskContext context, string input)
        {
            TInput parameter = default(TInput);
            JArray jArray = JArray.Parse(input);
            if (jArray != null)
            {
                int parameterCount = jArray.Count;
                if (parameterCount > 1)
                {
                    throw new TaskFailureException(
                        "TaskActivity implementation cannot be invoked due to more than expected input parameters.  Signature mismatch.");
                }

                if (parameterCount == 1)
                {
                    JToken jToken = jArray[0];
                    var jValue = jToken as JValue;
                    if (jValue != null)
                    {
                        parameter = jValue.ToObject<TInput>();
                    }
                    else
                    {
                        string serializedValue = jToken.ToString();
                        parameter = DataConverter.Deserialize<TInput>(serializedValue);
                    }
                }
            }

            TResult result;
            try
            {
                result = await ExecuteAsync(context, parameter);
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                string details = Utils.SerializeCause(e, DataConverter);
                throw new TaskFailureException(e.Message, e, details);
            }

            string serializedResult = DataConverter.Serialize(result);
            return serializedResult;
        }
    }

    /// <summary>
    ///     Typed base class for creating typed sync task activities
    /// </summary>
    /// <typeparam name="TInput">Input type for the activity</typeparam>
    /// <typeparam name="TResult">Output type of the activity</typeparam>
    public abstract class TaskActivity<TInput, TResult> : AsyncTaskActivity<TInput, TResult>
    {
        /// <summary>
        /// Abstract method for executing a task activity syncronously
        /// </summary>
        /// <param name="context">The task context</param>
        /// <param name="input">The typed input</param>
        /// <returns>The typed output from the execution</returns>
        protected abstract TResult Execute(TaskContext context, TInput input);

        /// <summary>
        /// Method for executing a task activity asyncronously
        /// </summary>
        /// <param name="context">The task context</param>
        /// <param name="input">The typed input</param>
        /// <returns>The typed output from the execution</returns>
        protected override Task<TResult> ExecuteAsync(TaskContext context, TInput input)
        {
            return Task.FromResult(Execute(context, input));
        }
    }
}