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
    using DurableTask.Core;
    using Microsoft.Extensions.DependencyInjection;

#if NETFRAMEWORK
    using System.Net;
    using System.Web.Http;
    using System.Web.Http.ExceptionHandling;
    using Owin;

    class Startup : IOwinAppBuilder
    {
        FabricOrchestrationProvider fabricOrchestrationProvider;
        string listeningAddress;

        public Startup(string listeningAddress, FabricOrchestrationProvider fabricOrchestrationProvider)
        {
            this.listeningAddress = listeningAddress ?? throw new ArgumentNullException(nameof(listeningAddress));
            this.fabricOrchestrationProvider = fabricOrchestrationProvider ?? throw new ArgumentNullException(nameof(fabricOrchestrationProvider));
        }

        public string GetListeningAddress()
        {
            return this.listeningAddress;
        }

        private ServiceProvider GenerateServiceProvider()
        {
            var services = new ServiceCollection();
            services.AddSingleton<IOrchestrationServiceClient>(this.fabricOrchestrationProvider.OrchestrationServiceClient);
            services.AddTransient<FabricOrchestrationServiceController>();
            services.AddSingleton<IExceptionLogger, ProxyServiceExceptionLogger>();
            services.AddSingleton<IExceptionHandler, ProxyServiceExceptionHandler>();
            var serviceProvider = services.BuildServiceProvider();
            return serviceProvider;
        }

        void IOwinAppBuilder.Startup(IAppBuilder appBuilder)
        {
            ServicePointManager.DefaultConnectionLimit = 256;
            HttpConfiguration config = new HttpConfiguration();
            config.MapHttpAttributeRoutes();
            config.DependencyResolver = new DefaultDependencyResolver(GenerateServiceProvider());
            config.MessageHandlers.Add(new ActivityLoggingMessageHandler());
            config.IncludeErrorDetailPolicy = IncludeErrorDetailPolicy.Always;
            config.Formatters.Remove(config.Formatters.XmlFormatter);
            config.Formatters.Remove(config.Formatters.FormUrlEncodedFormatter);
            config.Formatters.JsonFormatter.SerializerSettings.TypeNameHandling = Newtonsoft.Json.TypeNameHandling.All;
            appBuilder.UseWebApi(config);
        }
    }
#else
    using Microsoft.AspNetCore.Builder;
    using Microsoft.AspNetCore.Hosting;
    using Newtonsoft.Json;

    class Startup
    {
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddSingleton<IOrchestrationServiceClient>(sp =>
                sp.GetRequiredService<FabricOrchestrationProvider>().OrchestrationServiceClient);
            services.AddControllers()
                .AddNewtonsoftJson(options =>
                {
                    options.SerializerSettings.TypeNameHandling = TypeNameHandling.All;
                });
        }

        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            app.UseMiddleware<ActivityLoggingMiddleware>();
            app.UseRouting();
            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });
        }
    }
#endif
}
