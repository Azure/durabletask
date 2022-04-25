// ---------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ---------------------------------------------------------------

namespace DurableTask.Core
{
    using System.Threading.Tasks;

    /// <summary>
    /// Generic extensions to create orchestrations type-safely.
    /// </summary>
    public static class TaskHubClientExtensions
    {
        /// <summary>
        /// Create a new orchestration of the specified type with an automatically generated instance id
        /// </summary>
        /// <typeparam name="TOrchestration">Type that derives from TaskOrchestration{string, string}</typeparam>
        /// <param name="taskHubClient"></param>
        /// <param name="input">Input parameter to the specified TaskOrchestration</param>
        /// <returns>OrchestrationInstance that represents the orchestration that was created</returns>
        public static Task<OrchestrationInstance> CreateOrchestrationInstanceAsync<TOrchestration>(this TaskHubClient taskHubClient, string input)
            where TOrchestration : TaskOrchestration<string, string>
        {
            return taskHubClient.CreateOrchestrationInstanceAsync(typeof(TOrchestration), input);
        }

        /// <summary>
        /// Create a new orchestration of the specified type with an automatically generated instance id
        /// </summary>
        /// <typeparam name="TOrchestration">Type that derives from TaskOrchestration</typeparam>
        /// <typeparam name="TInput">TaskOrchestration parameter type</typeparam>
        /// <typeparam name="TResult">TaskOrchestration result type</typeparam>
        /// <param name="taskHubClient"></param>
        /// <param name="input">Input parameter to the specified TaskOrchestration</param>
        /// <returns>OrchestrationInstance that represents the orchestration that was created</returns>
        public static Task<OrchestrationInstance> CreateOrchestrationInstanceAsync<TOrchestration, TInput, TResult>(this TaskHubClient taskHubClient, TInput input)
            where TOrchestration : TaskOrchestration<TInput, TResult>
        {
            return taskHubClient.CreateOrchestrationInstanceAsync(typeof(TOrchestration), input);
        }

        /// <summary>
        /// Create a new orchestration of the specified type with the specified instance id
        /// </summary>
        /// <typeparam name="TOrchestration">Type that derives from TaskOrchestration</typeparam>
        /// <typeparam name="TInput">TaskOrchestration parameter type</typeparam>
        /// <typeparam name="TResult">TaskOrchestration result type</typeparam>
        /// <param name="taskHubClient"></param>
        /// <param name="instanceId">Instance id for the orchestration to be created, must be unique across the Task Hub</param>
        /// <param name="input">Input parameter to the specified TaskOrchestration</param>
        /// <returns>OrchestrationInstance that represents the orchestration that was created</returns>
        public static Task<OrchestrationInstance> CreateOrchestrationInstanceAsync<TOrchestration, TInput, TResult>(this TaskHubClient taskHubClient, string instanceId, TInput input)
            where TOrchestration : TaskOrchestration<TInput, TResult>
        {
            return taskHubClient.CreateOrchestrationInstanceAsync(typeof(TOrchestration), instanceId, input);
        }

        /// <summary>
        /// Create a new orchestration of the specified type with the specified instance id
        /// </summary>
        /// <typeparam name="TOrchestration">Type that derives from TaskOrchestration</typeparam>
        /// <typeparam name="TInput">TaskOrchestration parameter type</typeparam>
        /// <typeparam name="TResult">TaskOrchestration result type</typeparam>
        /// <param name="taskHubClient"></param>
        /// <param name="instanceId">Instance id for the orchestration to be created, must be unique across the Task Hub</param>
        /// <param name="input">Input parameter to the specified TaskOrchestration</param>
        /// <param name="dedupeStatuses">States of previous orchestration executions to be considered while de-duping new orchestrations on the client</param>
        /// <returns>OrchestrationInstance that represents the orchestration that was created</returns>
        public static Task<OrchestrationInstance> CreateOrchestrationInstanceAsync<TOrchestration, TInput, TResult>(this TaskHubClient taskHubClient,
            string instanceId,
            TInput input,
            OrchestrationStatus[] dedupeStatuses)
            where TOrchestration : TaskOrchestration<TInput, TResult>
        {
            return taskHubClient.CreateOrchestrationInstanceAsync(typeof(TOrchestration), instanceId, input, dedupeStatuses);
        }
    }
}
