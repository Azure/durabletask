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
    using System.Reflection;
    using System.Threading.Tasks;
    using DurableTask.Common;
    using DurableTask.Exceptions;
    using DurableTask.Serializing;
    using Newtonsoft.Json.Linq;

    public class ReflectionBasedTaskActivity : TaskActivity
    {
        public ReflectionBasedTaskActivity(object activityObject, MethodInfo methodInfo)
        {
            DataConverter = new JsonDataConverter();
            this.activityObject = activityObject;
            MethodInfo = methodInfo;
        }

        public DataConverter DataConverter { get; private set; }
        public object activityObject { get; private set; }
        public MethodInfo MethodInfo { get; private set; }

        public override string Run(TaskContext context, string input)
        {
            // will never run
            return string.Empty;
        }

        public override async Task<string> RunAsync(TaskContext context, string input)
        {
            JArray jArray = JArray.Parse(input);
            int parameterCount = jArray.Count;
            ParameterInfo[] methodParameters = MethodInfo.GetParameters();
            if (methodParameters.Length < parameterCount)
            {
                throw new TaskFailureException(
                    "TaskActivity implementation cannot be invoked due to more than expected input parameters.  Signature mismatch.");
            }
            var inputParameters = new object[methodParameters.Length];
            for (int i = 0; i < methodParameters.Length; i++)
            {
                Type parameterType = methodParameters[i].ParameterType;
                if (i < parameterCount)
                {
                    JToken jToken = jArray[i];
                    var jValue = jToken as JValue;
                    if (jValue != null)
                    {
                        inputParameters[i] = jValue.ToObject(parameterType);
                    }
                    else
                    {
                        string serializedValue = jToken.ToString();
                        inputParameters[i] = DataConverter.Deserialize(serializedValue, parameterType);
                    }
                }
                else
                {
                    if (methodParameters[i].HasDefaultValue)
                    {
                        inputParameters[i] = Type.Missing;
                    }
                    else
                    {
                        inputParameters[i] = parameterType.IsValueType ? Activator.CreateInstance(parameterType) : null;
                    }
                }
            }

            string serializedReturn;
            try
            {
                object invocationResult = InvokeActivity(inputParameters);
                if (invocationResult is Task)
                {
                    var invocationTask = invocationResult as Task;
                    if (MethodInfo.ReturnType.IsGenericType)
                    {
                        serializedReturn = DataConverter.Serialize(await ((dynamic) invocationTask));
                    }
                    else
                    {
                        await invocationTask;
                        serializedReturn = string.Empty;
                    }
                }
                else
                {
                    serializedReturn = DataConverter.Serialize(invocationResult);
                }
            }
            catch (TargetInvocationException e)
            {
                Exception realException = e.InnerException ?? e;
                string details = Utils.SerializeCause(realException, DataConverter);
                throw new TaskFailureException(realException.Message, details);
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                string details = Utils.SerializeCause(e, DataConverter);
                throw new TaskFailureException(e.Message, details);
            }

            return serializedReturn;
        }

        public virtual object InvokeActivity(object[] inputParameters)
        {
            return MethodInfo.Invoke(activityObject, inputParameters);
        }
    }
}