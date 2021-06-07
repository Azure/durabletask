// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.App
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using Microsoft.AspNetCore.Builder;
    using Microsoft.AspNetCore.Hosting;
    using Microsoft.AspNetCore.Server.Kestrel.Core;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Logging;

    public class TaskHubServer
    {
        readonly INameVersionObjectManager<TaskOrchestration> orchestrationManager;
        readonly INameVersionObjectManager<TaskActivity> activityManager;
        readonly ILogger logger;

        IWebHost? host;

        public TaskHubServer(
            INameVersionObjectManager<TaskOrchestration>? orchestrationManager = null,
            INameVersionObjectManager<TaskActivity>? activityManager = null,
            ILoggerFactory? loggerFactory = null)
        {
            loggerFactory ??= GetDefaultLoggerFactory();
            this.logger = loggerFactory.CreateLogger("DurableTask.App");
            this.orchestrationManager = orchestrationManager ?? new DefaultNameVersionObjectManager<TaskOrchestration>(this.logger);
            this.activityManager = activityManager ?? new DefaultNameVersionObjectManager<TaskActivity>(this.logger);
        }

        public void AddOrchestrationCreator(ObjectCreator<TaskOrchestration> creator)
        {
            this.orchestrationManager.Add(creator ?? throw new ArgumentNullException(nameof(creator)));
        }

        public void AddActivityCreator(ObjectCreator<TaskActivity> creator)
        {
            this.activityManager.Add(creator ?? throw new ArgumentNullException(nameof(creator)));
        }

        public Task StartAsync(string baseUri) => this.StartAsync(new Uri(baseUri));

        public async Task StartAsync(Uri? baseUri = null)
        {
            baseUri ??= new Uri("http://localhost:4000");

            this.host?.Dispose();

            this.host = new WebHostBuilder()
                .UseKestrel(options =>
                {
                    // Trying to use Http1AndHttp2 causes http2 connections to fail with invalid protocol error
                    // according to Microsoft dual http version mode not supported in unencrypted scenario:
                    // https://docs.microsoft.com/en-us/aspnet/core/grpc/troubleshoot?view=aspnetcore-3.0
                    options.ConfigureEndpointDefaults(listenOptions => listenOptions.Protocols = HttpProtocols.Http2);
                })
                .UseUrls(baseUri.OriginalString)
                .ConfigureServices(services =>
                {
                    services.AddGrpc();
                    services.AddSingleton(new TaskHubGrpcServer(this.orchestrationManager, this.activityManager));
                })
                .Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints =>
                    {
                        endpoints.MapGrpcService<TaskHubGrpcServer>();
                    });
                })
                .Build();
            await this.host.StartAsync();

            this.logger.LogInformation($"Listening on {baseUri}...");
        }

        public Task StopAsync()
        {
            return this.host?.StopAsync() ?? Task.CompletedTask;
        }

        static ILoggerFactory GetDefaultLoggerFactory()
        {
            return LoggerFactory.Create(builder =>
            {
                builder.AddSimpleConsole(options =>
                {
                    options.SingleLine = true;
                    options.UseUtcTimestamp = true;
                    options.TimestampFormat = "yyyy-mm-ddThh:mm:ss.ffffffZ ";
                });
            });
        }

        // Copied from C:\GitHub\durabletask\src\DurableTask.Core\NameVersionObjectManager.cs
        class DefaultNameVersionObjectManager<T> : INameVersionObjectManager<T>
        {
            readonly IDictionary<string, ObjectCreator<T>> creators;
            readonly ILogger logger;

            readonly object thisLock = new();

            public DefaultNameVersionObjectManager(ILogger logger)
            {
                this.creators = new Dictionary<string, ObjectCreator<T>>(StringComparer.OrdinalIgnoreCase);
                this.logger = logger;
            }

            public void Add(ObjectCreator<T> creator)
            {
                lock (this.thisLock)
                {
                    string key = GetKey(creator.Name, creator.Version);
                    if (!this.creators.TryAdd(key, creator))
                    {
                        throw new InvalidOperationException($"Duplicate entry detected: {creator.Name} {creator.Version}");
                    }

                    this.logger.LogInformation($"Registered {typeof(T).Name} '{creator.Name}'");
                }
            }

            public T GetObject(string name, string version)
            {
                string key = GetKey(name, version);

                lock (this.thisLock)
                {
                    if (this.creators.TryGetValue(key, out ObjectCreator<T>? creator))
                    {
                        return creator.Create();
                    }

                    return default!;
                }
            }

            static string GetKey(string name, string version)
            {
                return name + "_" + version;
            }
        }
    }
}
