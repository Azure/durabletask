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
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Threading.Tasks;
    using DurableTask.Core.Common;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.Serializing;
    using Newtonsoft.Json.Linq;

    /// <summary>
    /// Reflection based task activity for interface based task activities
    /// </summary>
    public class ReflectionBasedTaskActivity : TaskActivity
    {
        private DataConverter dataConverter;
        private readonly Type[] genericArguments;

        /// <summary>
        /// Creates a new ReflectionBasedTaskActivity based on an activity object and method info
        /// </summary>
        /// <param name="activityObject">The activity object to invoke methods on</param>
        /// <param name="methodInfo">The Reflection.methodInfo for invoking the method on the activity object</param>
        public ReflectionBasedTaskActivity(object activityObject, MethodInfo methodInfo)
        {
            DataConverter = JsonDataConverter.Default;
            ActivityObject = activityObject;
            MethodInfo = methodInfo;
            genericArguments = methodInfo.GetGenericArguments();
        }

        /// <summary>
        /// The DataConverter to use for input and output serialization/deserialization
        /// </summary>
        public DataConverter DataConverter
        {
            get => dataConverter;
            set => dataConverter = value ?? throw new ArgumentNullException(nameof(value));
        }

        /// <summary>
        /// The activity object to invoke methods on
        /// </summary>
        public object ActivityObject { get; private set; }

        /// <summary>
        /// The Reflection.methodInfo for invoking the method on the activity object
        /// </summary>
        public MethodInfo MethodInfo { get; private set; }

        /// <summary>
        /// Synchronous execute method, blocked for AsyncTaskActivity
        /// </summary>
        /// <returns>string.Empty</returns>
        public override string Run(TaskContext context, string input)
        {
            // will never run
            return string.Empty;
        }

        /// <summary>
        /// Method for executing a task activity asynchronously
        /// </summary>
        /// <param name="context">The task context</param>
        /// <param name="input">The serialized input</param>
        /// <returns>Serialized output from the execution</returns>
        public override async Task<string> RunAsync(TaskContext context, string input)
        {
            var jArray = Utils.ConvertToJArray(input);

            int parameterCount = jArray.Count - this.genericArguments.Length;
            ParameterInfo[] methodParameters = MethodInfo.GetParameters();
            if (methodParameters.Length < parameterCount)
            {
                throw new TaskFailureException(
                    "TaskActivity implementation cannot be invoked due to more than expected input parameters.  Signature mismatch.")
                    .WithFailureSource(MethodInfoString());
            }

            Type[] genericTypeArguments = this.GetGenericTypeArguments(jArray);
            object[] inputParameters = this.GetInputParameters(jArray, parameterCount, methodParameters, genericTypeArguments);

            string serializedReturn = string.Empty;
            Exception exception = null;
            try
            {
                object invocationResult = InvokeActivity(inputParameters, genericTypeArguments);
                if (invocationResult is Task invocationTask)
                {
                    if (this.MethodInfo.ReturnType.IsGenericType)
                    {
                        await invocationTask;

                        Type returnType = Utils.GetGenericReturnType(this.MethodInfo, genericTypeArguments);
                        PropertyInfo resultProperty = typeof(Task<>).MakeGenericType(returnType).GetProperty("Result");
                        serializedReturn = this.DataConverter.Serialize(resultProperty.GetValue(invocationTask));
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
                exception = e.InnerException ?? e;
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                exception = e;
            }

            if (exception != null)
            {
                string details = null;
                FailureDetails failureDetails = null;
                if (context.ErrorPropagationMode == ErrorPropagationMode.SerializeExceptions)
                {
                    details = Utils.SerializeCause(exception, DataConverter);
                }
                else
                {
                    failureDetails = new FailureDetails(exception);
                }

                throw new TaskFailureException(exception.Message, exception, details)
                    .WithFailureSource(MethodInfoString())
                    .WithFailureDetails(failureDetails);
            }

            return serializedReturn;
        }

        /// <summary>
        /// Invokes the target method on the activity object with supplied parameters
        /// </summary>
        /// <param name="inputParameters"></param>
        /// <returns></returns>
        public virtual object InvokeActivity(object[] inputParameters)
        {
            return MethodInfo.Invoke(ActivityObject, inputParameters);
        }

        /// <summary>
        /// Invokes the target method on the activity object with supplied parameters
        /// </summary>
        /// <param name="inputParameters">The methods' input parameters.</param>
        /// <param name="genericTypeParameters">The methods' generic parameters.</param>
        public virtual object InvokeActivity(object[] inputParameters, Type[] genericTypeParameters)
        {
            if (genericTypeParameters.Any())
            {
                return MethodInfo.MakeGenericMethod(genericTypeParameters).Invoke(ActivityObject, inputParameters);
            }

            return MethodInfo.Invoke(ActivityObject, inputParameters);
        }

        string MethodInfoString()
        {
            return $"{MethodInfo.ReflectedType?.FullName}.{MethodInfo.Name}";
        }

        private Type[] GetGenericTypeArguments(JArray jArray)
        {
            List<Type> genericParameters = new List<Type>(this.genericArguments.Length);

            for (int i = jArray.Count - this.genericArguments.Length; i < jArray.Count; i++)
            {
                Utils.TypeMetadata typeMetadata = jArray[i].ToObject<Utils.TypeMetadata>();
                genericParameters.Add(Assembly.Load(typeMetadata.AssemblyName).GetType(typeMetadata.FullyQualifiedTypeName));
            }

            return genericParameters.ToArray();
        }

        private object[] GetInputParameters(JArray jArray, int parameterCount, ParameterInfo[] methodParameters, Type[] genericArguments)
        {
            var inputParameters = new object[methodParameters.Length];
            for (var i = 0; i < methodParameters.Length; i++)
            {
                Type parameterType = Utils.ConvertFromGenericType(
                    this.MethodInfo.GetGenericArguments(),
                    genericArguments,
                    methodParameters[i].ParameterType);

                if (i < parameterCount)
                {
                    JToken jToken = jArray[i];
                    if (jToken is JValue jValue)
                    {
                        inputParameters[i] = jValue.ToObject(parameterType);
                    }
                    else
                    {
                        string serializedValue = jToken.ToString();
                        inputParameters[i] = this.DataConverter.Deserialize(serializedValue, parameterType);
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

            return inputParameters;
        }
    }
}