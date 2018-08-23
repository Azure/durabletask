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
    /// <summary>
    /// Class to hold context for a WorkItemDispatcher call
    /// </summary>
    public class WorkItemDispatcherContext
    {
        /// <summary>
        /// Creates a new instance of the WorkItemDispatcherContext class
        /// </summary>
        /// <param name="name">The context name</param>
        /// <param name="id">The context id</param>
        /// <param name="dispatcherId">The context dispatcher id</param>
        public WorkItemDispatcherContext(string name, string id, string dispatcherId)
        {
            this.Name = name;
            this.Id = id;
            this.DispatcherId = dispatcherId;
        }

        /// <summary>
        /// Gets the name from the context
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Gets the id from the context
        /// </summary>
        public string Id { get; private set; }

        /// <summary>
        /// Gets the dispatcher id from the context
        /// </summary>
        public string DispatcherId { get; private set; }
    }
}
