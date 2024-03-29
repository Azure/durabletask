﻿//  ----------------------------------------------------------------------------------
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

namespace DurableTask.Core.Tests
{
    using System;
    using System.Runtime.Serialization;
    using System.Text;

    public static class TestUtils
    {
        public static string GenerateRandomString(int length)
        {
            var result = new StringBuilder(length);
            while (result.Length < length)
            {
                // Use GUIDs so these don't compress well
                result.Append(Guid.NewGuid().ToString("N"));
            }

            return result.ToString(0, length);
        }

        /// <summary>
        /// A minimalistic serializable class.
        /// </summary>
        [DataContract]
        public class DummyMessage
        {
            public DummyMessage(int eventId, string name)
            {
                this.Name = name;
                this.EventId = eventId;
            }

            public DummyMessage()
            { }

            [DataMember]
            public string Name { get; private set; }

            [DataMember]
            public int EventId { get; private set; }
        }
    }
}
