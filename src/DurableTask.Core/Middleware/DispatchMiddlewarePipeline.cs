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
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    class DispatchMiddlewarePipeline
    {
        readonly IList<Func<DispatchMiddlewareDelegate, DispatchMiddlewareDelegate>> components = 
            new List<Func<DispatchMiddlewareDelegate, DispatchMiddlewareDelegate>>();

        public Task RunAsync(DispatchMiddlewareContext context, DispatchMiddlewareDelegate handler)
        {
            // Build the delegate chain
            foreach (var component in this.components.Reverse())
            {
                handler = component(handler);
            }

            return handler(context);
        }

        public void Add(Func<DispatchMiddlewareContext, Func<Task>, Task> middleware)
        {
            this.components.Add(next =>
            {
                return context =>
                {
                    Func<Task> simpleNext = () => next(context);
                    return middleware(context, simpleNext);
                };
            });
        }
    }
}
