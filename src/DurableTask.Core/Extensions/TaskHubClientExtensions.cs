// ---------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ---------------------------------------------------------------

namespace DurableTask.Core
{
    using System;
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
        public static Task<OrchestrationInstance> CreateOrchestrationInstanceAsync<TOrchestration, TInput, TResult>(
            this TaskHubClient taskHubClient,
            string instanceId,
            TInput input,
            OrchestrationStatus[] dedupeStatuses)
            where TOrchestration : TaskOrchestration<TInput, TResult>
        {
            return taskHubClient.CreateOrchestrationInstanceAsync(typeof(TOrchestration), instanceId, input, dedupeStatuses);
        }

        /// <summary>
        ///     Creates an orchestration instance, and raises an event for it, which eventually causes the OnEvent() method in the
        ///     orchestration to fire.
        /// </summary>
        /// <typeparam name="TOrchestration">Type that derives from TaskOrchestration</typeparam>
        /// <typeparam name="TInput">TaskOrchestration parameter type</typeparam>
        /// <typeparam name="TResult">TaskOrchestration result type</typeparam>
        /// <param name="taskHubClient"></param>
        /// <param name="orchestrationInput">Input parameter to the specified TaskOrchestration</param>
        /// <param name="eventName">Name of the event</param>
        /// <param name="eventData">Data for the event</param>
        /// <returns>OrchestrationInstance that represents the orchestration that was created</returns>
        /// 
        public static Task<OrchestrationInstance> CreateOrchestrationInstanceWithRaisedEventAsync<TOrchestration, TInput, TResult>(
            this TaskHubClient taskHubClient,
            TInput orchestrationInput,
            string eventName,
            object eventData)
            where TOrchestration : TaskOrchestration<TInput, TResult>
        {
            return taskHubClient.CreateOrchestrationInstanceWithRaisedEventAsync(typeof(TOrchestration), orchestrationInput, eventName, eventData);
        }

        /// <summary>
        ///     Creates an orchestration instance, and raises an event for it, which eventually causes the OnEvent() method in the
        ///     orchestration to fire.
        /// </summary>
        /// <typeparam name="TOrchestration">Type that derives from TaskOrchestration</typeparam>
        /// <typeparam name="TInput">TaskOrchestration parameter type</typeparam>
        /// <typeparam name="TResult">TaskOrchestration result type</typeparam>
        /// <param name="taskHubClient"></param>
        /// <param name="instanceId">Instance id for the orchestration to be created, must be unique across the Task Hub</param>
        /// <param name="orchestrationInput">Input parameter to the specified TaskOrchestration</param>
        /// <param name="eventName">Name of the event</param>
        /// <param name="eventData">Data for the event</param>
        /// <returns>OrchestrationInstance that represents the orchestration that was created</returns>
        public static Task<OrchestrationInstance> CreateOrchestrationInstanceWithRaisedEventAsync<TOrchestration, TInput, TResult>(
            this TaskHubClient taskHubClient,
            string instanceId,
            TInput orchestrationInput,
            string eventName,
            object eventData)
            where TOrchestration : TaskOrchestration<TInput, TResult>
        {
            return taskHubClient.CreateOrchestrationInstanceWithRaisedEventAsync(typeof(TOrchestration), instanceId, orchestrationInput, eventName, eventData);
        }

        /// <summary>
        ///     Creates an orchestration instance, and raises an event for it, which eventually causes the OnEvent() method in the
        ///     orchestration to fire.
        /// </summary>
        /// <typeparam name="TOrchestration">Type that derives from TaskOrchestration</typeparam>
        /// <typeparam name="TInput">TaskOrchestration parameter type</typeparam>
        /// <typeparam name="TResult">TaskOrchestration result type</typeparam>
        /// <param name="taskHubClient"></param>
        /// <param name="instanceId">Instance id for the orchestration to be created, must be unique across the Task Hub</param>
        /// <param name="orchestrationInput">Input parameter to the specified TaskOrchestration</param>
        /// <param name="dedupeStatuses">States of previous orchestration executions to be considered while de-duping new orchestrations on the client</param>
        /// <param name="eventName">Name of the event</param>
        /// <param name="eventData">Data for the event</param>
        /// <returns>OrchestrationInstance that represents the orchestration that was created</returns>
        public static Task<OrchestrationInstance> CreateOrchestrationInstanceWithRaisedEventAsync<TOrchestration, TInput, TResult>(
            this TaskHubClient taskHubClient,
            string instanceId,
            TInput orchestrationInput,
            OrchestrationStatus[] dedupeStatuses,
            string eventName,
            object eventData)
            where TOrchestration : TaskOrchestration<TInput, TResult>
        {
            return taskHubClient.CreateOrchestrationInstanceWithRaisedEventAsync(typeof(TOrchestration), instanceId, orchestrationInput, eventName, eventData);
        }

        /// <summary>
        ///     Create a new orchestration of the specified type with the specified instance id, scheduled to start at an specific time
        /// </summary>
        /// <typeparam name="TOrchestration">Type that derives from TaskOrchestration</typeparam>
        /// <typeparam name="TInput">TaskOrchestration parameter type</typeparam>
        /// <typeparam name="TResult">TaskOrchestration result type</typeparam>
        /// <param name="taskHubClient"></param>
        /// <param name="input">Input parameter to the specified TaskOrchestration</param>
        /// <param name="startAt">Orchestration start time</param>
        /// <returns>OrchestrationInstance that represents the orchestration that was created</returns>
        public static Task<OrchestrationInstance> CreateScheduledOrchestrationInstanceAsync<TOrchestration, TInput, TResult>(
            this TaskHubClient taskHubClient,
            TInput input,
            DateTime startAt)
            where TOrchestration : TaskOrchestration<TInput, TResult>
        {
            return taskHubClient.CreateScheduledOrchestrationInstanceAsync(typeof(TOrchestration), input, startAt);
        }

        /// <summary>
        ///     Create a new orchestration of the specified type with the specified instance id, scheduled to start at an specific time
        /// </summary>
        /// <typeparam name="TOrchestration">Type that derives from TaskOrchestration</typeparam>
        /// <typeparam name="TInput">TaskOrchestration parameter type</typeparam>
        /// <typeparam name="TResult">TaskOrchestration result type</typeparam>
        /// <param name="taskHubClient"></param>
        /// <param name="instanceId">Instance id for the orchestration to be created, must be unique across the Task Hub</param>
        /// <param name="input">Input parameter to the specified TaskOrchestration</param>
        /// <param name="startAt">Orchestration start time</param>
        /// <returns>OrchestrationInstance that represents the orchestration that was created</returns>
        public static Task<OrchestrationInstance> CreateScheduledOrchestrationInstanceAsync<TOrchestration, TInput, TResult>(
            this TaskHubClient taskHubClient,
            string instanceId,
            TInput input,
            DateTime startAt)
            where TOrchestration : TaskOrchestration<TInput, TResult>
        {
            return taskHubClient.CreateScheduledOrchestrationInstanceAsync(typeof(TOrchestration), instanceId, input, startAt);
        }
    }
}