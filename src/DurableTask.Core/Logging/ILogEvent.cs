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

namespace DurableTask.Core.Logging
{
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Basic interface that defines the basic requirements for all DurableTask
    /// log messages for <see cref="ILogger"/> compatibility.
    /// </summary>
    public interface ILogEvent
    {
        /// <summary>
        /// The ID of the log event. This must match one of the values in <see cref="EventIds"/>.
        /// </summary>
        EventId EventId { get; }

        /// <summary>
        /// The level of the log event.
        /// </summary>
        LogLevel Level { get; }

        /// <summary>
        /// Gets the message to write to the <see cref="ILogger"/> infrastructure.
        /// This method will not be called if the corresponding event is filtered out.
        /// </summary>
        string FormattedMessage { get; }
    }
}
