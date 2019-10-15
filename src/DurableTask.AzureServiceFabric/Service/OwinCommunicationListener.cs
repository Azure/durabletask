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
    using System.Threading;
    using System.Threading.Tasks;

    using DurableTask.AzureServiceFabric.Tracing;

    using Microsoft.Owin.Hosting;
    using Microsoft.ServiceFabric.Services.Communication.Runtime;

    /// <summary>
    /// Provides <see cref="ICommunicationListener"/> with support for Owin.
    /// </summary>
    public class OwinCommunicationListener : ICommunicationListener
    {
        /// <summary>
        /// OWIN server handle.
        /// </summary>
        private IDisposable serverHandle;

        private IOwinAppBuilder owinAppBuilder;

        /// <summary>
        ///
        /// </summary>
        /// <param name="owinAppBuilder">Owin Application builder</param>
        public OwinCommunicationListener(IOwinAppBuilder owinAppBuilder)
        {
            this.owinAppBuilder = owinAppBuilder;
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task<string> OpenAsync(CancellationToken cancellationToken)
        {
            var listeningAddress = this.owinAppBuilder.GetListeningAddress();
            ServiceFabricProviderEventSource.Tracing.TraceMessage(nameof(OwinCommunicationListener), $"Opening on {listeningAddress}");

            try
            {
                this.serverHandle = WebApp.Start(listeningAddress, appBuilder => this.owinAppBuilder.Startup(appBuilder));
                return Task.FromResult(listeningAddress);
            }
            catch (Exception ex)
            {
                ServiceFabricProviderEventSource.Tracing.UnexpectedCodeCondition($"{ex.Message} {ex.StackTrace} {ex.InnerException}");
                this.StopWebServer();
                throw;
            }
        }

        /// <summary>
        /// Close Owin service.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task CloseAsync(CancellationToken cancellationToken)
        {
            ServiceFabricProviderEventSource.Tracing.TraceMessage(nameof(OwinCommunicationListener), "Listener is closing");
            this.StopWebServer();
            return Task.CompletedTask;
        }

        /// <summary>
        /// Abort service.
        /// </summary>
        public void Abort()
        {
            ServiceFabricProviderEventSource.Tracing.TraceMessage(nameof(OwinCommunicationListener), "Listener is aborting");
            this.StopWebServer();
        }

        private void StopWebServer()
        {
            if (this.serverHandle != null)
            {
                try
                {
                    this.serverHandle.Dispose();
                }
                catch (ObjectDisposedException)
                {
                    // no-op
                }
            }
        }
    }
}
