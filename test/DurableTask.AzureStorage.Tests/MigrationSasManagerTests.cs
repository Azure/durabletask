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
#nullable enable
namespace DurableTask.AzureStorage.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Xml.Linq;
    using Azure;
    using Azure.Core;
    using Azure.Core.Pipeline;
    using Azure.Data.Tables;
    using Azure.Storage.Blobs;
    using Azure.Storage.Queues;
    using DurableTask.AzureStorage.Storage;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json.Linq;

    [TestClass]
    public class MigrationSasManagerTests
    {
        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task Renewal_UpdatesCachedCredentialsUsingStorageTime(bool sharedKey)
        {
            // 1. Issue credentials from the prepared control row.
            using StorageResponses storage = new StorageResponses(sharedKey);
            MigrationSasManager manager = storage.CreateManager();
            await manager.RefreshAsync(default);
            (char Service, string Resource)[] resources = { ('b', "payloads"), ('q', "testhub-control-00"), ('t', "TestHubHistory") };
            AzureSasCredential[] credentials = resources.Select(resource => manager.GetCredential(resource.Service, resource.Resource)).ToArray();
            string[] original = credentials.Select(credential => credential.Signature).ToArray();
            Assert.AreEqual(storage.ServerTime.AddSeconds(15), storage.Expiry);
            Assert.AreEqual(sharedKey ? 0 : 3, storage.KeyRequests);
            Assert.AreNotSame(credentials[1], manager.GetCredential('q', "testhub-control-01"));

            // 2. Renew without replacing cached credential objects or fetching new keys.
            storage.ServerTime = storage.ServerTime.AddSeconds(5);
            await manager.RefreshAsync(default);
            for (int index = 0; index < resources.Length; index++)
            {
                (char service, string resource) = resources[index];
                Assert.AreSame(credentials[index], manager.GetCredential(service, resource));
                Assert.AreNotEqual(original[index], credentials[index].Signature);
                Dictionary<string, string> query = Query(credentials[index].Signature);
                Assert.AreEqual(storage.ServerTime.AddSeconds(15), DateTimeOffset.Parse(query["se"], CultureInfo.InvariantCulture));
                Assert.IsFalse(string.IsNullOrEmpty(query["sig"]));
                if (!sharedKey)
                {
                    Assert.AreEqual("https", query["spr"]);
                    Assert.AreEqual(service.ToString(), query["sks"]);
                    Assert.AreEqual(query["skt"], query["st"]);
                }
            }
            Assert.AreEqual(sharedKey ? 0 : 3, storage.KeyRequests);
            Dictionary<string, string> tableQuery = Query(credentials[2].Signature);
            if (sharedKey)
            {
                Assert.AreEqual("t", tableQuery["ss"]);
                Assert.AreEqual("co", tableQuery["srt"]);
            }
            else
            {
                Assert.AreEqual("TestHubHistory", tableQuery["tn"]);
            }
        }

        [DataTestMethod]
        [DataRow(3, false)]
        [DataRow(4, true)]
        [DataRow(9, true)]
        public async Task Renewal_UsesTheElevenSecondMargin(int elapsedSeconds, bool renew)
        {
            // 1. Publish the first deadline, then advance the Storage clock.
            using StorageResponses storage = new StorageResponses();
            MigrationSasManager manager = storage.CreateManager();
            await manager.RefreshAsync(default);
            DateTimeOffset expiry = storage.Expiry;
            int writes = storage.GateWrites;
            storage.ServerTime = storage.ServerTime.AddSeconds(elapsedSeconds);

            // 2. Renew only when eleven seconds or less remain.
            await manager.RefreshAsync(default);
            Assert.AreEqual(writes + (renew ? 1 : 0), storage.GateWrites);
            Assert.AreEqual(renew ? storage.ServerTime.AddSeconds(15) : expiry, storage.Expiry);
        }

        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task Renewal_RechecksTheGateAfterLosingAnEtagRace(bool ending)
        {
            // 1. Another host changes the row after renewal reads it.
            using StorageResponses storage = new StorageResponses();
            MigrationSasManager manager = storage.CreateManager();
            await manager.RefreshAsync(default);
            DateTimeOffset originalExpiry = storage.Expiry;
            int writes = storage.GateWrites;
            storage.ServerTime = storage.ServerTime.AddSeconds(5);
            storage.BeforeWrite = async () =>
            {
                AzureStorageMigration.Gate gate = (await storage.Control.ReadAsync(default, storage.MigrationId))!;
                gate.IsMigrationEnding = ending;
                if (!ending) gate.AccessExpiresAt = storage.ServerTime.AddSeconds(30);
                Assert.IsTrue(await storage.Control.TryUpdateAsync(gate, default));
            };

            // 2. The stale write loses; the latest row determines access and expiry.
            await manager.RefreshAsync(default);
            Assert.AreEqual(1, storage.Conflicts);
            Assert.AreEqual(writes + 1, storage.GateWrites);
            Assert.AreEqual(ending ? originalExpiry : storage.ServerTime.AddSeconds(30), storage.Expiry);
            Assert.AreEqual(ending, manager.IsMigrationEnding);
            if (ending)
            {
                Assert.ThrowsException<OrchestrationServiceUnavailableException>(() => manager.EnsureAccess());
                Assert.ThrowsException<OrchestrationServiceUnavailableException>(() => manager.GetCredential('q', "queue"));
            }
            else
            {
                Assert.AreEqual(storage.Expiry, DateTimeOffset.Parse(Query(manager.GetCredential('q', "queue").Signature)["se"], CultureInfo.InvariantCulture));
            }
        }

        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task Refresh_ObservesDrainBeforeStartupOrDuringKeyAcquisition(bool beforeStartup)
        {
            // 1. Close the row before startup or while a key request is in flight.
            using StorageResponses storage = new StorageResponses();
            MigrationSasManager manager = storage.CreateManager();
            if (beforeStartup) await storage.EndAsync();
            else storage.BeforeKey = _ => storage.EndAsync();

            // 2. No deadline is published, and ending is reported only once.
            await manager.RefreshAsync(default);
            await manager.RefreshAsync(default);
            Assert.IsTrue(manager.IsMigrationEnding);
            Assert.AreEqual(1, storage.EndingCalls);
            Assert.AreEqual(1, storage.GateWrites);
            Assert.AreEqual(DateTimeOffset.FromUnixTimeSeconds(0), storage.Expiry);
            Assert.AreEqual(beforeStartup ? 0 : 3, storage.KeyRequests);
            Assert.ThrowsException<OrchestrationServiceUnavailableException>(() => manager.EnsureAccess());
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task Refresh_MissingGateDoesNotCreateOrRenewIt(bool initialized)
        {
            // 1. Remove the row before startup or after credentials have been issued.
            using StorageResponses storage = new StorageResponses();
            MigrationSasManager manager = storage.CreateManager();
            if (initialized) await manager.RefreshAsync(default);
            int writes = storage.GateWrites;
            storage.Row = null;

            // 2. Renewal fails without recreating the deleted row.
            await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => manager.RefreshAsync(default));
            Assert.IsNull(storage.Row);
            Assert.AreEqual(writes, storage.GateWrites);
            Assert.AreEqual(initialized ? 3 : 0, storage.KeyRequests);
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task Refresh_FailureDoesNotExtendCredentialsAndCanBeRetried(bool failKey)
        {
            // 1. Fail a renewal after the original credentials have been issued.
            using StorageResponses storage = new StorageResponses();
            MigrationSasManager manager = storage.CreateManager();
            await manager.RefreshAsync(default);
            AzureSasCredential credential = manager.GetCredential('q', "queue");
            string signature = credential.Signature;
            DateTimeOffset expiry = storage.Expiry;
            storage.ServerTime = failKey ? storage.ServerTime.AddHours(24) : storage.ServerTime.AddSeconds(5);
            storage.FailKeys = failKey;
            storage.FailReads = !failKey;
            await Assert.ThrowsExceptionAsync<RequestFailedException>(() => manager.RefreshAsync(default));
            Assert.AreEqual(expiry, storage.Expiry);
            Assert.AreEqual(signature, credential.Signature);

            // 2. Retry with healthy storage and update the same credential.
            storage.FailReads = storage.FailKeys = false;
            await manager.RefreshAsync(default);
            Assert.AreSame(credential, manager.GetCredential('q', "queue"));
            Assert.AreEqual(storage.ServerTime.AddSeconds(15), storage.Expiry);
            Assert.AreNotEqual(signature, credential.Signature);
        }

        [TestMethod]
        public async Task Refresh_RereadsStorageTimeAfterSlowKeyAcquisition()
        {
            // 1. Advance Storage's clock while fetching a delegation key.
            using StorageResponses storage = new StorageResponses();
            MigrationSasManager manager = storage.CreateManager();
            storage.BeforeKey = _ =>
            {
                storage.ServerTime = storage.ServerTime.AddMinutes(1);
                return Task.CompletedTask;
            };

            // 2. Use a fresh gate read instead of publishing an already expired deadline.
            await manager.RefreshAsync(default);
            Assert.AreEqual(storage.ServerTime.AddSeconds(15), storage.Expiry);
            Assert.AreEqual(1, storage.GateWrites);
            Assert.AreEqual(3, storage.KeyRequests);
        }

        [TestMethod]
        [Timeout(10000)]
        public async Task Refresh_CanceledWaiterDoesNotCancelTheCurrentRefresh()
        {
            // 1. Hold the first refresh in key acquisition.
            using StorageResponses storage = new StorageResponses();
            MigrationSasManager manager = storage.CreateManager();
            TaskCompletionSource<bool> started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource<bool> release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            storage.BeforeKey = async _ => { started.SetResult(true); await release.Task; };
            Task first = manager.RefreshAsync(default);
            await started.Task;
            try
            {
                // 2. Cancel a second caller while it waits for the first refresh.
                using CancellationTokenSource cancellation = new CancellationTokenSource();
                Task second = manager.RefreshAsync(cancellation.Token);
                cancellation.Cancel();
                await AssertCanceledAsync(second);
                Assert.IsFalse(first.IsCompleted);
            }
            finally
            {
                release.TrySetResult(true);
                await first;
            }
            Assert.AreEqual(1, storage.GateWrites);
            Assert.AreEqual(3, storage.KeyRequests);
        }

        [TestMethod]
        [Timeout(10000)]
        public async Task Refresh_CancellationDuringKeyAcquisitionAllowsAnotherRefresh()
        {
            // 1. Block key acquisition until its caller cancels.
            using StorageResponses storage = new StorageResponses();
            MigrationSasManager manager = storage.CreateManager();
            TaskCompletionSource<bool> started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            storage.BeforeKey = async token => { started.SetResult(true); await Task.Delay(Timeout.Infinite, token); };
            using CancellationTokenSource cancellation = new CancellationTokenSource();
            Task pending = manager.RefreshAsync(cancellation.Token);
            await started.Task;
            cancellation.Cancel();
            await AssertCanceledAsync(pending);
            Assert.AreEqual(0, storage.GateWrites);

            // 2. Cancellation releases the refresh lock.
            await manager.RefreshAsync(default);
            Assert.AreEqual(storage.ServerTime.AddSeconds(15), storage.Expiry);
        }

        [TestMethod]
        [Timeout(10000)]
        public async Task RefreshLoop_CanStopAndRestartAndStopsWhenMigrationEnds()
        {
            // 1. Repeated starts share one loop, which can be stopped and restarted.
            using StorageResponses storage = new StorageResponses();
            MigrationSasManager manager = storage.CreateManager();
            await manager.RefreshAsync(default);
            Task first = manager.StartRefreshLoop();
            try
            {
                Assert.AreSame(first, manager.StartRefreshLoop());
                await manager.StopRefreshing();
                Task second = manager.StartRefreshLoop();
                Assert.AreNotSame(first, second);
                await manager.StopRefreshing();

                // 2. The running loop notices drain and ends without another start.
                await storage.EndAsync();
                await manager.StartRefreshLoop();
                Assert.IsTrue(manager.IsMigrationEnding);
                Assert.AreEqual(1, storage.EndingCalls);
                Assert.IsTrue(manager.StartRefreshLoop().IsCompleted);
            }
            finally
            {
                await manager.StopRefreshing();
            }
        }

        static async Task AssertCanceledAsync(Task task)
        {
            try
            {
                await task;
                Assert.Fail("The caller should observe cancellation.");
            }
            catch (OperationCanceledException) { }
        }

        static Dictionary<string, string> Query(string signature) => signature.TrimStart('?').Split('&')
            .Select(part => part.Split(new[] { '=' }, 2))
            .ToDictionary(parts => parts[0], parts => Uri.UnescapeDataString(parts[1]));

        // Real SDK clients use this local handler; no requests reach Azure.
        sealed class StorageResponses : HttpPipelineTransport, IDisposable
        {
            readonly StorageAccountClientProvider provider;
            int revision;

            public StorageResponses(bool sharedKey = false)
            {
                TestCredential credential = new TestCredential();
                string connection = "DefaultEndpointsProtocol=https;AccountName=account;AccountKey=" + Convert.ToBase64String(new byte[32]) + ";EndpointSuffix=core.windows.net";
                this.provider = sharedKey
                    ? new StorageAccountClientProvider(
                        StorageServiceClientProvider.ForBlob(connection, this.Options(new BlobClientOptions())),
                        StorageServiceClientProvider.ForQueue(connection, this.Options(new QueueClientOptions())),
                        StorageServiceClientProvider.ForTable(connection, this.Options(new TableClientOptions())))
                    : new StorageAccountClientProvider(
                        StorageServiceClientProvider.ForBlob("account", credential, this.Options(new BlobClientOptions())),
                        StorageServiceClientProvider.ForQueue("account", credential, this.Options(new QueueClientOptions())),
                        StorageServiceClientProvider.ForTable("account", credential, this.Options(new TableClientOptions())));
                this.Control = new AzureStorageMigration(this.provider.Table.CreateClient(this.provider.Table.CreateOptions()), "TestHub");
                this.Row = new JObject
                {
                    ["PartitionKey"] = string.Empty,
                    ["RowKey"] = this.MigrationId.ToString("D"),
                    ["MigrationId@odata.type"] = "Edm.Guid",
                    ["MigrationId"] = this.MigrationId,
                    ["IsDraining"] = false,
                    ["AccessExpiresAt@odata.type"] = "Edm.DateTime",
                    ["AccessExpiresAt"] = DateTimeOffset.FromUnixTimeSeconds(0),
                    ["odata.etag"] = "v0",
                };
            }

            public Guid MigrationId { get; } = Guid.NewGuid();
            public DateTimeOffset ServerTime { get; set; } = new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);
            public JObject? Row { get; set; }
            public DateTimeOffset Expiry => this.Row!["AccessExpiresAt"]!.ToObject<DateTimeOffset>();
            public int GateWrites { get; private set; }
            public int KeyRequests { get; private set; }
            public int Conflicts { get; private set; }
            public int EndingCalls { get; private set; }
            public bool FailReads { get; set; }
            public bool FailKeys { get; set; }
            public Func<Task>? BeforeWrite { get; set; }
            public Func<CancellationToken, Task>? BeforeKey { get; set; }
            public AzureStorageMigration Control { get; }

            public MigrationSasManager CreateManager() => new MigrationSasManager(
                new AzureStorageOrchestrationServiceSettings { TaskHubName = "TestHub", StorageAccountClientProvider = this.provider },
                this.MigrationId,
                this.provider.Blob.CreateClient(this.provider.Blob.CreateOptions()),
                this.provider.Queue.CreateClient(this.provider.Queue.CreateOptions()),
                this.provider.Table.CreateClient(this.provider.Table.CreateOptions()),
                this.provider.Table,
                () => this.EndingCalls++);

            public async Task EndAsync()
            {
                AzureStorageMigration.Gate gate = (await this.Control.ReadAsync(default, this.MigrationId))!;
                gate.IsMigrationEnding = true;
                Assert.IsTrue(await this.Control.TryUpdateAsync(gate, default));
            }

            T Options<T>(T options) where T : ClientOptions
            {
                options.Transport = this;
                options.Retry.MaxRetries = 0;
                return options;
            }

            async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            {
                Uri uri = request.RequestUri!;
                if (uri.Query.Contains("comp=userdelegationkey"))
                {
                    this.KeyRequests++;
                    Func<CancellationToken, Task>? hook = this.BeforeKey;
                    this.BeforeKey = null;
                    if (hook is not null) await hook(cancellationToken);
                    if (this.FailKeys) return this.Error(HttpStatusCode.ServiceUnavailable, "ServerBusy");
                    string service = uri.Host.Contains(".table.") ? "t" : uri.Host.Contains(".queue.") ? "q" : "b";
                    XElement body = new XElement("UserDelegationKey",
                        new XElement("SignedOid", "11111111-1111-1111-1111-111111111111"),
                        new XElement("SignedTid", "22222222-2222-2222-2222-222222222222"),
                        new XElement("SignedStart", this.ServerTime.AddMinutes(-15).ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture)),
                        new XElement("SignedExpiry", this.ServerTime.AddHours(24).ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture)),
                        new XElement("SignedService", service),
                        new XElement("SignedVersion", "2025-07-05"),
                        new XElement("Value", Convert.ToBase64String(new byte[32])));
                    return this.Response(HttpStatusCode.OK, body.ToString(), "application/xml");
                }

                Assert.AreEqual($"/TestHubMigrationCtrl(PartitionKey='',RowKey='{this.MigrationId:D}')", Uri.UnescapeDataString(uri.AbsolutePath));
                if (request.Method == HttpMethod.Get)
                {
                    if (this.FailReads) return this.Error(HttpStatusCode.ServiceUnavailable, "ServerBusy");
                    return this.Row is null ? this.Error(HttpStatusCode.NotFound, "ResourceNotFound")
                        : this.Response(HttpStatusCode.OK, this.Row.ToString());
                }

                Assert.AreEqual(HttpMethod.Put, request.Method, "The manager must not create the control row.");
                Func<Task>? beforeWrite = this.BeforeWrite;
                this.BeforeWrite = null;
                if (beforeWrite is not null) await beforeWrite();
                if (this.Row is null) return this.Error(HttpStatusCode.NotFound, "ResourceNotFound");
                if (request.Headers.GetValues("If-Match").Single() != this.Row.Value<string>("odata.etag"))
                {
                    this.Conflicts++;
                    return this.Error(HttpStatusCode.PreconditionFailed, "UpdateConditionNotSatisfied");
                }
                this.Row = JObject.Parse(await request.Content!.ReadAsStringAsync());
                Assert.AreEqual(this.MigrationId, this.Row["MigrationId"]!.ToObject<Guid>());
                this.Row["odata.etag"] = "v" + (++this.revision).ToString(CultureInfo.InvariantCulture);
                this.GateWrites++;
                return this.Response(HttpStatusCode.NoContent);
            }

            HttpResponseMessage Response(HttpStatusCode status, string body = "", string type = "application/json")
            {
                HttpResponseMessage response = new HttpResponseMessage(status)
                {
                    Content = new StreamContent(new ResponseStream(Encoding.UTF8.GetBytes(body))),
                };
                response.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue(type);
                response.Headers.Date = this.ServerTime;
                response.Headers.TryAddWithoutValidation("ETag", this.Row?.Value<string>("odata.etag") ?? "v0");
                return response;
            }

            HttpResponseMessage Error(HttpStatusCode status, string code)
            {
                HttpResponseMessage response = this.Response(status, new JObject
                {
                    ["odata.error"] = new JObject { ["code"] = code, ["message"] = new JObject { ["lang"] = "en-US", ["value"] = code } },
                }.ToString());
                response.Headers.Add("x-ms-error-code", code);
                return response;
            }

            public override Request CreateRequest() => HttpClientTransport.Shared.CreateRequest();

            public override void Process(HttpMessage message) => this.ProcessAsync(message).AsTask().GetAwaiter().GetResult();

            public override async ValueTask ProcessAsync(HttpMessage message)
            {
                message.CancellationToken.ThrowIfCancellationRequested();
                using HttpRequestMessage request = new HttpRequestMessage(new HttpMethod(message.Request.Method.Method), message.Request.Uri.ToUri());
                if (message.Request.Content is not null)
                {
                    using MemoryStream body = new MemoryStream();
                    await message.Request.Content.WriteToAsync(body, message.CancellationToken);
                    request.Content = new ByteArrayContent(body.ToArray());
                }
                foreach (HttpHeader header in message.Request.Headers)
                {
                    if (!request.Headers.TryAddWithoutValidation(header.Name, header.Value))
                    {
                        request.Content?.Headers.TryAddWithoutValidation(header.Name, header.Value);
                    }
                }
                using HttpResponseMessage response = await this.SendAsync(request, message.CancellationToken);
                message.Response = new StorageResponse(response, await response.Content.ReadAsByteArrayAsync());
            }

            public void Dispose() { }
        }

        sealed class StorageResponse : Response
        {
            readonly Dictionary<string, string> headers;

            public StorageResponse(HttpResponseMessage response, byte[] body)
            {
                this.Status = (int)response.StatusCode;
                this.ReasonPhrase = response.ReasonPhrase ?? string.Empty;
                this.ContentStream = new ResponseStream(body);
                this.headers = response.Headers.Concat(response.Content.Headers)
                    .ToDictionary(header => header.Key, header => string.Join(",", header.Value), StringComparer.OrdinalIgnoreCase);
            }

            public override int Status { get; }
            public override string ReasonPhrase { get; }
            public override Stream? ContentStream { get; set; }
            public override string ClientRequestId { get; set; } = string.Empty;
            public override void Dispose() { }
            protected override bool ContainsHeader(string name) => this.headers.ContainsKey(name);
            protected override bool TryGetHeader(string name, out string value) => this.headers.TryGetValue(name, out value!);
            protected override bool TryGetHeaderValues(string name, out IEnumerable<string> values)
            {
                bool found = this.headers.TryGetValue(name, out string? value);
                values = found ? new[] { value! } : Array.Empty<string>();
                return found;
            }
            protected override IEnumerable<HttpHeader> EnumerateHeaders() => this.headers.Select(pair => new HttpHeader(pair.Key, pair.Value));
        }

        // Keep the response body available until the SDK has finished reading it.
        sealed class ResponseStream : MemoryStream
        {
            public ResponseStream(byte[] bytes) : base(bytes) { }
            protected override void Dispose(bool disposing) { }
        }

        sealed class TestCredential : TokenCredential
        {
            public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return new AccessToken("test-token", DateTimeOffset.UtcNow.AddHours(1));
            }

            public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
                new ValueTask<AccessToken>(this.GetToken(requestContext, cancellationToken));
        }
    }
}
