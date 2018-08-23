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

namespace DurableTask.Core.Serializing
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Threading.Tasks;
    using DurableTask.Core.Common;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Core.Settings;
    using DurableTask.Core.Tracing;
    using DurableTask.Core.Tracking;

    /// <summary>
    /// A converter that does conversion between the OrchestrationRuntimeState instance and a stream after serialization.
    /// The stream is a serialized OrchestrationSessionState that will set as session state.
    /// De-serialization is done with fallbacks in the order: OrchestrationSessionState -> OrchestrationRuntimeState -> IList of HistoryEvent.
    /// </summary>
    public class RuntimeStateStreamConverter
    {
        /// <summary>
        /// Convert an OrchestrationRuntimeState instance to a serialized raw stream to be saved in session state.
        /// </summary>
        /// <param name="newOrchestrationRuntimeState">The new OrchestrationRuntimeState to be serialized</param>
        /// <param name="runtimeState">The current runtime state</param>
        /// <param name="dataConverter">A data converter for serialization and deserialization</param>
        /// <param name="shouldCompress">True if should compress when serialization</param>
        /// <param name="serviceBusSessionSettings">The service bus session settings</param>
        /// <param name="orchestrationServiceBlobStore">A blob store for external blob storage</param>
        /// <param name="sessionId">The session id</param>
        /// <returns>A serialized raw strem to be saved in session state</returns>
        public static async Task<Stream> OrchestrationRuntimeStateToRawStream(
            OrchestrationRuntimeState newOrchestrationRuntimeState,
            OrchestrationRuntimeState runtimeState,
            DataConverter dataConverter,
            bool shouldCompress,
            ISessionSettings serviceBusSessionSettings,
            IOrchestrationServiceBlobStore orchestrationServiceBlobStore,
            string sessionId)
        {
            OrchestrationSessionState orchestrationSessionState = new OrchestrationSessionState(newOrchestrationRuntimeState.Events);
            string serializedState = dataConverter.Serialize(orchestrationSessionState);

            long originalStreamSize = 0;
            Stream compressedState = Utils.WriteStringToStream(
                serializedState,
                shouldCompress,
                out originalStreamSize);

            runtimeState.Size = originalStreamSize;
            runtimeState.CompressedSize = compressedState.Length;

            if (runtimeState.CompressedSize > serviceBusSessionSettings.SessionMaxSizeInBytes)
            {
                throw new OrchestrationException($"Session state size of {runtimeState.CompressedSize} exceeded the termination threshold of {serviceBusSessionSettings.SessionMaxSizeInBytes} bytes");
            }

            if (runtimeState.CompressedSize > serviceBusSessionSettings.SessionOverflowThresholdInBytes)
            {
                TraceHelper.TraceSession(
                    TraceEventType.Information,
                    "RuntimeStateStreamConverter-SessionStateThresholdExceeded",
                    sessionId,
                    $"Session state size of {runtimeState.CompressedSize} exceeded the termination threshold of {serviceBusSessionSettings.SessionOverflowThresholdInBytes} bytes." +
                    $"Creating an OrchestrationSessionState instance with key for exteranl storage.");
                return await CreateStreamForExternalStorageAsync(shouldCompress, orchestrationServiceBlobStore, sessionId, dataConverter, compressedState);
            }

            return compressedState;
        }

        async static Task<Stream> CreateStreamForExternalStorageAsync(
            bool shouldCompress,
            IOrchestrationServiceBlobStore orchestrationServiceBlobStore,
            string sessionId,
            DataConverter dataConverter,
            Stream compressedState)
        {
            if (orchestrationServiceBlobStore == null)
            {
                throw new OrchestrationException(
                    "The compressed session is larger than supported. " +
                    "Please provide an implementation of IOrchestrationServiceBlobStore for external storage.");
            }

            // create a new orchestration session state with the external blob key
            string key = orchestrationServiceBlobStore.BuildSessionBlobKey(sessionId);
            TraceHelper.TraceSession(
                TraceEventType.Information,
                "RuntimeStateStreamConverter-SaveSessionToStorage",
                sessionId,
                $"Saving the serialized stream in external storage with key {key}.");

            // save the compressedState stream externally as a blob
            await orchestrationServiceBlobStore.SaveStreamAsync(key, compressedState);

            // create an OrchestrationSessionState instance to hold the blob key,
            // and then serialize the instance as a sream for the session state
            OrchestrationSessionState orchestrationSessionState = new OrchestrationSessionState(key);
            string serializedStateExternal = dataConverter.Serialize(orchestrationSessionState);

            long streamSize;
            Stream compressedStateForSession = Utils.WriteStringToStream(
                serializedStateExternal,
                shouldCompress,
                out streamSize);
            return compressedStateForSession;
        }

        /// <summary>
        /// Convert a raw stream to an orchestration runtime state instance.
        /// </summary>
        /// <param name="rawSessionStream">The raw session stream to be deserialized</param>
        /// <param name="sessionId">The session Id</param>
        /// <param name="orchestrationServiceBlobStore">A blob store for external blob storage</param>
        /// <param name="dataConverter">>A data converter for serialization and deserialization</param>
        /// <returns></returns>
        public static async Task<OrchestrationRuntimeState> RawStreamToRuntimeState(Stream rawSessionStream, string sessionId, IOrchestrationServiceBlobStore orchestrationServiceBlobStore, DataConverter dataConverter)
        {
            bool isEmptySession;
            OrchestrationRuntimeState runtimeState;
            Stream sessionStream = await Utils.GetDecompressedStreamAsync(rawSessionStream);

            isEmptySession = sessionStream == null;
            long rawSessionStateSize = isEmptySession ? 0 : rawSessionStream.Length;
            long newSessionStateSize = isEmptySession ? 0 : sessionStream.Length;

            string blobKey;
            runtimeState = GetOrCreateInstanceState(sessionStream, sessionId, dataConverter, out blobKey);

            if (string.IsNullOrWhiteSpace(blobKey))
            {         
                TraceHelper.TraceSession(
                    TraceEventType.Information,
                    "RuntimeStateStreamConverter-StreamToRuntimeStateSize",
                    sessionId,
                    $"Size of session state is {newSessionStateSize}, compressed {rawSessionStateSize}");
                return runtimeState;
            }

            if (orchestrationServiceBlobStore == null)
            {
                throw new OrchestrationException(
                    $"Please provide an implementation of IOrchestrationServiceBlobStore for external storage to load the runtime state.");
            }

            TraceHelper.TraceSession(
                TraceEventType.Information,
                "RuntimeStateStreamConverter-StreamToRuntimeStateLoadFromStorage",
                sessionId,
                $"Loading the serialzied stream from external storage with blob key {blobKey}.");

            Stream externalStream = await orchestrationServiceBlobStore.LoadStreamAsync(blobKey);
            return await RawStreamToRuntimeState(externalStream, sessionId, orchestrationServiceBlobStore, dataConverter);
        }

        static OrchestrationRuntimeState GetOrCreateInstanceState(Stream stateStream, string sessionId, DataConverter dataConverter, out string blobKey)
        {
            OrchestrationRuntimeState runtimeState;
            blobKey = string.Empty;
            if (stateStream == null)
            {
                TraceHelper.TraceSession(
                    TraceEventType.Information,
                    "RuntimeStateStreamConverter-GetOrCreateInstanceStateNewSession",
                    sessionId,
                    "No session state exists, creating new session state.");
                runtimeState = new OrchestrationRuntimeState();
            }
            else
            {
                if (stateStream.Position != 0)
                {
                    throw TraceHelper.TraceExceptionSession(
                        TraceEventType.Error,
                        "RuntimeStateStreamConverter-GetOrCreateInstanceStatePartiallyConsumed",
                        sessionId,
                        new ArgumentException("Stream is partially consumed"));
                }

                string serializedState = null;
                using (var reader = new StreamReader(stateStream))
                {
                    serializedState = reader.ReadToEnd();
                }

                runtimeState = DeserializeToRuntimeStateWithFallback(serializedState, dataConverter, sessionId, out blobKey);
            }

            return runtimeState;
        }

        /// <summary>
        /// Deserialize the session state to construct an OrchestrationRuntimeState instance.
        ///
        /// The session state string could be one of these:
        ///     1. a serialized IList of HistoryEvent (master branch implementation), or
        ///     2. a serialized OrchestrationRuntimeState instance with the history event list (vnext branch implementation), or
        ///     3. a serialized OrchestrationSessionState instance with the history event list or a blob key (latest implementation).
        ///
        /// So when doing the deserialization, it is done with fallbacks in the order: OrchestrationSessionState -> OrchestrationRuntimeState -> IList of HistoryEvent, to cover all cases.
        ///
        /// </summary>
        /// <param name="serializedState">The serialized session state</param>
        /// <param name="dataConverter">A data converter for serialization and deserialization</param>
        /// <param name="sessionId">The session Id</param>
        /// <param name="blobKey">The blob key output. Will be set if the state is in external storage.</param>
        /// <returns>The converted orchestration runtime state.</returns>
        static OrchestrationRuntimeState DeserializeToRuntimeStateWithFallback(string serializedState, DataConverter dataConverter, string sessionId, out string blobKey)
        {
            OrchestrationRuntimeState runtimeState = null;
            blobKey = string.Empty;
            try
            {
                var sessionState =
                    dataConverter.Deserialize<OrchestrationSessionState>(serializedState);
                runtimeState = new OrchestrationRuntimeState(sessionState.Events);
                blobKey = sessionState.BlobKey;
            }
            catch (Exception exception)
            {
                TraceHelper.TraceSession(
                    TraceEventType.Warning,
                    "RuntimeStateStreamConverter-DeserializeToRuntimeStateFailed",
                    sessionId,
                    $"Failed to deserialize session state to OrchestrationSessionState object: {serializedState}. More info: {exception.StackTrace}");
                try
                {
                    var restoredState =
                        dataConverter.Deserialize<OrchestrationRuntimeState>(serializedState);
                    // Create a new Object with just the events, we don't want the rest
                    runtimeState = new OrchestrationRuntimeState(restoredState.Events);
                }
                catch (Exception e)
                {
                    TraceHelper.TraceSession(
                        TraceEventType.Warning,
                        "RuntimeStateStreamConverter-DeserializeToRuntimeStateException",
                        sessionId,
                        $"Failed to deserialize session state to OrchestrationRuntimeState object: {serializedState}. More info: {e.StackTrace}");

                    var events = dataConverter.Deserialize<IList<HistoryEvent>>(serializedState);
                    runtimeState = new OrchestrationRuntimeState(events);
                }
            }

            return runtimeState;
        }
    }
}
