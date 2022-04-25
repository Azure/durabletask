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
            where TActivity: TaskActivity<TInput, TResult>
        {
            return orchestrationContext.ScheduleTask<TResult>(typeof(TActivity), parameters);
        }
    }
}