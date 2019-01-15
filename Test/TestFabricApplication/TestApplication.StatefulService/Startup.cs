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

namespace TestApplication.StatefulService
{
    using System.Net;
    using System.Web.Http;

    using DurableTask.Core;
    using DurableTask.ServiceFabric;
    using Microsoft.Extensions.DependencyInjection;
    using Owin;

    class Startup : IOwinAppBuilder
    {
        FabricOrchestrationProvider fabricOrchestrationProvider;

        public Startup(FabricOrchestrationProvider fabricOrchestrationProvider)
        {
            this.fabricOrchestrationProvider = fabricOrchestrationProvider;
        }

        public void Configuration(IAppBuilder appBuilder)
        {
            ServicePointManager.DefaultConnectionLimit = 256;
            HttpConfiguration config = new HttpConfiguration();
            config.DependencyResolver = new DefaultDependencyResolver(GenerateServiceProvider());

            config.Routes.MapHttpRoute(
                name: "DurableTasks",
                routeTemplate: "api/dtfx/{action}",
                defaults: new { controller = "FabricOrchestrationService", id = RouteParameter.Optional }
            );

            config.Formatters.JsonFormatter.SerializerSettings.TypeNameHandling = Newtonsoft.Json.TypeNameHandling.All;
            appBuilder.UseWebApi(config);
        }

        private ServiceProvider GenerateServiceProvider()
        {
            var services = new ServiceCollection();
            services.AddSingleton<IOrchestrationServiceClient>(this.fabricOrchestrationProvider.OrchestrationServiceClient);
            services.AddTransient<FabricOrchestrationServiceController>();
            var serviceProvider = services.BuildServiceProvider();
            return serviceProvider;
        }
    }
}
