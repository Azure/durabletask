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

namespace DurableTask.AzureServiceFabric.Service
{
    using System;
    using System.Collections.Generic;
    using System.Web.Http.Dependencies;

    using Microsoft.Extensions.DependencyInjection;

    /// <inheritdoc/>
    public sealed class DefaultDependencyResolver : IDependencyResolver
    {
        private IServiceProvider provider;

        /// <summary>
        /// Creates an instance of <see cref="DefaultDependencyResolver"/>.
        /// </summary>
        /// <param name="provider">An instance of <see cref="IServiceProvider"/> </param>
        public DefaultDependencyResolver(IServiceProvider provider)
        {
            this.provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        /// <inheritdoc/>
        public object GetService(Type serviceType)
        {
            return provider.GetService(serviceType);
        }

        /// <inheritdoc/>
        public IEnumerable<object> GetServices(Type serviceType)
        {
            return provider.GetServices(serviceType);
        }

        /// <inheritdoc/>
        public IDependencyScope BeginScope()
        {
            return this;
        }

        #region IDisposable Support
        /// <inheritdoc />
        public void Dispose()
        {
            // no-op
        }
        #endregion
    }
}
