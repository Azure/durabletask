// ---------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ---------------------------------------------------------------

namespace DurableTask.Core
{
    using System.Threading.Tasks;

    /// <summary>
    /// Generic extensions to create orchestration activities type-safely.
    /// </summary>
    public static class OrchestrationContextExtensions
    {
        /// <summary>
        /// Schedule a TaskActivity by type.
        /// </summary>
        /// <typeparam name="TActivity">Type that devices from TaskActivity class</typeparam>
        /// <typeparam name="TResult">Return Type of the TaskActivity.Execute method</typeparam>
        /// <typeparam name="TInput">Parameter type of TaskActivity</typeparam>
        /// <param name="orchestrationContext"></param>
        /// <param name="parameters">Parameters for the TaskActivity.Execute method</param>
        /// <returns>Task that represents the execution of the specified TaskActivity</returns>
        public static Task<TResult> ScheduleTask<TActivity, TInput, TResult>(this OrchestrationContext orchestrationContext, TInput parameters)
            where TActivity : TaskActivity<TInput, TResult>
        {
            return orchestrationContext.ScheduleTask<TResult>(typeof(TActivity), parameters);
        }
        
        /// <summary>
        ///     Schedule a TaskActivity by type. Also retry on failure as per supplied policy.
        /// </summary>
        /// <typeparam name="TActivity">Type that devices from TaskActivity class</typeparam>
        /// <typeparam name="TResult">Return Type of the TaskActivity.Execute method</typeparam>
        /// <typeparam name="TInput">Parameter type of TaskActivity</typeparam>
        /// <param name="orchestrationContext"></param>
        /// <param name="retryOptions">Retry policy</param>
        /// <param name="parameters">Parameters for the TaskActivity.Execute method</param>
        /// <returns>Task that represents the execution of the specified TaskActivity</returns>
        public static Task<TResult> ScheduleWithRetry<TActivity, TInput, TResult>(
            this OrchestrationContext orchestrationContext,
            RetryOptions retryOptions,
            params object[] parameters)
            where TActivity : TaskActivity<TInput, TResult>
        {
            return orchestrationContext.ScheduleWithRetry<TResult>(typeof(TActivity), retryOptions, parameters);
        }

        /// <summary>
        ///     Create a sub-orchestration of the specified type. Also retry on failure as per supplied policy.
        /// </summary>
        /// <typeparam name="TOrchestration">Type that derives from TaskOrchestration</typeparam>
        /// <typeparam name="TInput">TaskOrchestration parameter type</typeparam>
        /// <typeparam name="TResult">TaskOrchestration result type</typeparam>
        /// <param name="orchestrationContext"></param>
        /// <param name="retryOptions">Retry policy</param>
        /// <param name="input">Input for the TaskOrchestration.RunTask method</param>
        /// <returns>Task that represents the execution of the specified sub-orchestration</returns>
        public static Task<TResult> CreateSubOrchestrationInstanceWithRetry<TOrchestration, TInput, TResult>(
            this OrchestrationContext orchestrationContext,
            RetryOptions retryOptions,
            TInput input)
            where TOrchestration : TaskOrchestration<TInput, TResult>
        {
            return orchestrationContext.CreateSubOrchestrationInstanceWithRetry<TResult>(typeof(TOrchestration), retryOptions, input);
        }

        /// <summary>
        ///     Create a sub-orchestration of the specified type. Also retry on failure as per supplied policy.
        /// </summary>
        /// <typeparam name="TOrchestration">Type that derives from TaskOrchestration</typeparam>
        /// <typeparam name="TInput">TaskOrchestration parameter type</typeparam>
        /// <typeparam name="TResult">TaskOrchestration result type</typeparam>
        /// <param name="orchestrationContext"></param>
        /// <param name="instanceId">Instance Id of the sub-orchestration</param>
        /// <param name="retryOptions">Retry policy</param>
        /// <param name="input">Input for the TaskOrchestration.RunTask method</param>
        /// <returns>Task that represents the execution of the specified sub-orchestration</returns>
        public static Task<TResult> CreateSubOrchestrationInstanceWithRetry<TOrchestration, TInput, TResult>(
            this OrchestrationContext orchestrationContext,
            string instanceId,
            RetryOptions retryOptions,
            TInput input)
            where TOrchestration : TaskOrchestration<TInput, TResult>
        {
            return orchestrationContext.CreateSubOrchestrationInstanceWithRetry<TResult>(typeof(TOrchestration), instanceId, retryOptions, input);
        }

        /// <summary>
        ///     Create a sub-orchestration of the specified type.
        /// </summary>
        /// <typeparam name="TOrchestration">Type that derives from TaskOrchestration</typeparam>
        /// <typeparam name="TInput">TaskOrchestration parameter type</typeparam>
        /// <typeparam name="TResult">TaskOrchestration result type</typeparam>
        /// <param name="orchestrationContext"></param>
        /// <param name="input">Input for the TaskOrchestration.RunTask method</param>
        /// <returns>Task that represents the execution of the specified sub-orchestration</returns>
        public static Task<TResult> CreateSubOrchestrationInstance<TOrchestration, TInput, TResult>(this OrchestrationContext orchestrationContext, TInput input)
            where TOrchestration : TaskOrchestration<TInput, TResult>
        {
            return orchestrationContext.CreateSubOrchestrationInstance<TResult>(typeof(TOrchestration), input);
        }

        /// <summary>
        ///     Create a sub-orchestration of the specified type with the specified instance id
        /// </summary>
        /// <typeparam name="TOrchestration">Type that derives from TaskOrchestration</typeparam>
        /// <typeparam name="TInput">TaskOrchestration parameter type</typeparam>
        /// <typeparam name="TResult">TaskOrchestration result type</typeparam>
        /// <param name="orchestrationContext"></param>
        /// <param name="instanceId">InstanceId of the sub-orchestration to create</param>
        /// <param name="input">Input for the TaskOrchestration.RunTask method</param>
        /// <returns>Task that represents the execution of the specified sub-orchestration</returns>
        public static Task<TResult> CreateSubOrchestrationInstance<TOrchestration, TInput, TResult>(this OrchestrationContext orchestrationContext, string instanceId, TInput input)
            where TOrchestration : TaskOrchestration<TInput, TResult>
        {
            return orchestrationContext.CreateSubOrchestrationInstance<TResult>(typeof(TOrchestration), instanceId, input);
        }
    }
}