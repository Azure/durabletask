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
#nullable enable
namespace DurableTask.Core.Entities.EventFormat
{
    using System;

    /// <summary>
    /// Determines event names to use for messages sent to and from entities.
    /// </summary>
    internal static class EntityMessageEventNames
    {
        public static string RequestMessageEventName => "op";

        public static string ReleaseMessageEventName => "release";

        public static string ContinueMessageEventName => "continue";

        public static string ScheduledRequestMessageEventName(DateTime scheduledUtc) => $"op@{scheduledUtc:o}";

        public static string ResponseMessageEventName(Guid requestId) => requestId.ToString();

        public static bool IsRequestMessage(string eventName) => eventName.StartsWith("op");

        public static bool IsReleaseMessage(string eventName) => eventName == "release";
    }
}
