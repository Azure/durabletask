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

namespace TestApplication.StatefulService
{
    using System;
    using System.Diagnostics;
    using System.Fabric;
    using System.Security.Cryptography.X509Certificates;
    using System.Threading;

    using DurableTask.AzureServiceFabric.Service;

    using Microsoft.ServiceFabric.Services.Runtime;

    internal static class Program
    {
        private static string sslEndpointPort;

        /// <summary>
        /// This is the entry point of the service host process.
        /// </summary>
        private static void Main()
        {
            try
            {
                // The ServiceManifest.XML file defines one or more service type names.
                // Registering a service maps a service type name to a .NET type.
                // When Service Fabric creates an instance of this service type,
                // an instance of the class is created in this host process.
                ServiceRuntime.RegisterServiceAsync("StatefulServiceType", context =>
                {
                    BindSslPort(context);
                    var testProvider = new TestOrchestrationsProvider();
                    var listener = new TaskHubProxyListener(testProvider.GetFabricOrchestrationProviderSettings(), testProvider.RegisterOrchestrations);
                    return new TaskHubStatefulService(context, new[] { listener });
                }).GetAwaiter().GetResult();

                // Prevents this host process from terminating so services keep running.
                Thread.Sleep(Timeout.Infinite);
            }
            catch (Exception e)
            {
                Trace.WriteLine(e);
                throw;
            }
            finally
            {
                // Test purpose only.
                UnBindSslPort();
            }
        }

        private static void BindSslPort(StatefulServiceContext statefulServiceContext)
        {
            if (!string.IsNullOrWhiteSpace(sslEndpointPort))
            {
                return;
            }

            var endpoint = statefulServiceContext.CodePackageActivationContext.GetEndpoint("DtfxServiceEndpoint");
            X509Certificate2 certificate;

            using (var certStore = new X509Store(StoreName.My, StoreLocation.LocalMachine))
            {
                certStore.Open(OpenFlags.ReadOnly | OpenFlags.OpenExistingOnly);
                var certs = certStore.Certificates.Find(X509FindType.FindBySubjectDistinguishedName, "CN=localhost", false);
                certificate = certs[0];
            }

            sslEndpointPort = $"0.0.0.0:{endpoint.Port}";
            UnBindSslPort();
            // Register unbind when ctrl+c, ctrl+break happens.
            Console.CancelKeyPress += (_, __) => UnBindSslPort();
            var appid = "\"{C60263BE-E2BC-45E0-80B4-896D8A11C64C}\"";
            RunNetShCommand($"http add sslcert ipport={sslEndpointPort} certHash={certificate.Thumbprint} appid={appid}");
        }

        private static void UnBindSslPort()
        {
            RunNetShCommand($"http delete sslcert ipport={sslEndpointPort}");
        }

        private static void RunNetShCommand(string arguments)
        {
            var psi = new ProcessStartInfo
            {
                CreateNoWindow = true,
                UseShellExecute = false,
                RedirectStandardError = true,
                RedirectStandardOutput = true,
                FileName = "netsh",
                Arguments = arguments
            };

            using (Process netshCommand = Process.Start(psi))
            {
                netshCommand.WaitForExit();
            }
        }
    }
}
