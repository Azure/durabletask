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
    using DurableTask.AzureServiceFabric;
    using DurableTask.Core;
    using Microsoft.AspNetCore.Builder;
    using Microsoft.AspNetCore.Hosting;
    using Microsoft.Extensions.DependencyInjection;
    using Newtonsoft.Json;
    class Startup
    {
        readonly FabricOrchestrationProvider fabricOrchestrationProvider;

        public Startup(FabricOrchestrationProvider fabricOrchestrationProvider)
        {
            this.fabricOrchestrationProvider = fabricOrchestrationProvider ?? throw new ArgumentNullException(nameof(fabricOrchestrationProvider));
        }

        /// <summary>
        /// this method gets called by the runtime. Use this method to add services to the container.
        /// </summary>
        /// <param name="services">Services to be configured.</param>
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddSingleton<IOrchestrationServiceClient>(this.fabricOrchestrationProvider.OrchestrationServiceClient);
            services.AddTransient<FabricOrchestrationServiceController>();

            services.AddControllers().AddNewtonsoftJson(options =>
            {
                options.SerializerSettings.TypeNameHandling = TypeNameHandling.All;
            });
        }


        public void Configure(IApplicationBuilder appBuilder, IWebHostEnvironment env)
        {
            appBuilder.UseRouting();
            appBuilder.UseAuthorization();
            appBuilder.UseEndpoints(endpoints =>
            {

                endpoints.MapControllers();

            });
        }

    }
}
