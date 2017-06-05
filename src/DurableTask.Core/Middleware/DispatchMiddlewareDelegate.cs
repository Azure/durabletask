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

namespace DurableTask.Core.Middleware
{
    using System.Threading.Tasks;

    /// <summary>
    /// A function that runs in the task execution middleware pipeline.
    /// </summary>
    /// <param name="context">The <see cref="DispatchMiddlewareContext"/> for the task execution.</param>
    /// <returns>A task that represents the completion of the durable task execution.</returns>
    public delegate Task DispatchMiddlewareDelegate(DispatchMiddlewareContext context);
}
