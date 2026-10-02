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
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Core;
    using Azure.Core.Pipeline;
    using Azure.Data.Tables;
    using Azure.Storage.Blobs;
    using Azure.Storage.Queues;
    using DurableTask.AzureStorage.Storage;
    using DurableTask.Core.Exceptions;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json.Linq;

    [TestClass]
    public class MigrationSasTests
    {
        [TestMethod]
        public async Task CachedClients_UseRenewedCredentialsAndRejectRequestsAfterDrain()
        {
            using var storage = new StorageResponses();
            var client = new AzureStorageClient(storage.Settings, isMigrationActive: true);
            Func<Task>[] writes = CachedWrites(client);
            try
            {
                // A client-only host must initialize without AzureStorageOrchestrationService.StartAsync.
                await writes[0]();
                await client.StopMigrationTokenRefreshAsync();
                await writes[1]();
                await writes[2]();
                DateTimeOffset firstExpiry = storage.Expiry;
                Assert.AreEqual(storage.ServerTime.AddSeconds(10), firstExpiry);
                Assert.AreEqual(3, storage.DelegationRequests);
                AssertRequestsUseExpiry(storage.Workload, firstExpiry);

                // Move past the first expiry and renew SAS tokens using the same delegation keys.
                storage.ServerTime = storage.ServerTime.AddSeconds(20);
                await client.InitializeMigrationAsync(refresh: true);
                await client.StopMigrationTokenRefreshAsync();
                Assert.IsTrue(storage.Expiry > firstExpiry);
                Assert.AreEqual(3, storage.DelegationRequests, "Renewing SAS should reuse the delegation keys.");

                // The same cached clients must now send the new expiry on every request.
                storage.Workload.Clear();
                foreach (Func<Task> write in writes)
                {
                    await write();
                }
                AssertRequestsUseExpiry(storage.Workload, storage.Expiry);

                // After the client sees drain, cached clients must reject writes before sending them.
                await storage.EndMigrationAsync();
                await client.InitializeMigrationAsync(refresh: true);
                int sentBefore = storage.Workload.Count;
                foreach (Func<Task> write in writes)
                {
                    await Assert.ThrowsExceptionAsync<OrchestrationServiceUnavailableException>(write);
                }
                Assert.AreEqual(sentBefore, storage.Workload.Count, "Even previously cached clients must stop before sending a request.");
            }
            finally
            {
                await client.StopMigrationTokenRefreshAsync();
            }
        }

        [TestMethod]
        public async Task Renewal_LosingEtagRaceToDrainDoesNotExtendAccess()
        {
            using var storage = new StorageResponses();
            var client = new AzureStorageClient(storage.Settings, isMigrationActive: true);
            try
            {
                await client.InitializeMigrationAsync();
                await client.StopMigrationTokenRefreshAsync();
                // Save the last published deadline so we can check that drain does not extend it.
                DateTimeOffset finalExpiry = storage.Expiry;
                int writesBeforeDrain = storage.GateWrites;
                // Start drain after renewal reads the gate but before it writes the new expiry.
                // Setting the ServerTime to 5 seconds past will indicate that it is approaching the expiry
                // and should renew
                storage.ServerTime = storage.ServerTime.AddSeconds(5);
                storage.BeforeUpdate = storage.EndMigrationAsync;

                await client.InitializeMigrationAsync(refresh: true);

                // The stale renewal must lose its ETag check and leave the drain deadline unchanged.
                Assert.IsTrue(client.IsMigrationEnding);
                Assert.AreEqual(finalExpiry, storage.Expiry, "A stalled renewal must not extend the drain deadline.");
                Assert.AreEqual(writesBeforeDrain + 1, storage.GateWrites, "Only the drain update should commit.");
                Assert.AreEqual(1, storage.EtagConflicts);
            }
            finally
            {
                await client.StopMigrationTokenRefreshAsync();
            }
        }

        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task Initialization_ObservesDrainDuringCreationOrKeyAcquisition(bool duringCreation)
        {
            using var storage = new StorageResponses();
            var client = new AzureStorageClient(storage.Settings, isMigrationActive: true);
            // Inject drain while startup is creating the gate or fetching a delegation key.
            async Task endMigration()
            {
                await storage.Control.CreateIfNotExistsAsync(default);
                await storage.EndMigrationAsync();
            }
            if (duringCreation)
            {
                storage.BeforeInsert = endMigration;
            }
            else
            {
                storage.BeforeKeyResponse = endMigration;
            }

            try
            {
                await client.InitializeMigrationAsync();

                // Startup must notice drain without publishing a SAS deadline or allowing access.
                Assert.IsTrue(client.IsMigrationEnding);
                Assert.AreEqual(DateTimeOffset.FromUnixTimeSeconds(0), storage.Expiry, "No SAS deadline may be published after drain wins.");
                Assert.AreEqual(2, storage.GateWrites, "Only initial creation and drain should commit.");
                Assert.ThrowsException<OrchestrationServiceUnavailableException>(() => client.EnsureAccess());
                if (duringCreation)
                {
                    Assert.AreEqual(0, storage.DelegationRequests);
                }
            }
            finally
            {
                await client.StopMigrationTokenRefreshAsync();
            }
        }

        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task Initialization_CanRetryFailureButRenewalCannotRecreateMissingGate(bool initialFailure)
        {
            using var storage = new StorageResponses();
            var client = new AzureStorageClient(storage.Settings, isMigrationActive: true);
            try
            {
                if (initialFailure)
                {
                    // Fail the first startup attempt while it reads the gate.
                    storage.FailGateReads = true;
                    await Assert.ThrowsExceptionAsync<RequestFailedException>(() => client.InitializeMigrationAsync());
                    // Once reads work again, startup may retry and create the gate.
                    storage.FailGateReads = false;
                    await client.InitializeMigrationAsync();
                    Assert.IsTrue(storage.GateExists);
                    Assert.IsFalse(client.IsMigrationEnding);
                    Assert.AreEqual(storage.ServerTime.AddSeconds(10), storage.Expiry);
                }
                else
                {
                    await client.InitializeMigrationAsync();
                    await client.StopMigrationTokenRefreshAsync();
                    // Remove a gate that the client has already used successfully.
                    int writes = storage.GateWrites;
                    storage.GateExists = false;

                    // Renewal must fail instead of recreating the gate and allowing access again.
                    await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => client.InitializeMigrationAsync(refresh: true));

                    Assert.IsFalse(storage.GateExists);
                    Assert.AreEqual(writes, storage.GateWrites, "A missing gate during renewal must not restore access.");
                }
            }
            finally
            {
                await client.StopMigrationTokenRefreshAsync();
            }
        }

        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task Requests_DisableSdkRetriesAndBoundServerTimeoutOnlyDuringMigration(bool migrating)
        {
            using var storage = new StorageResponses();
            var client = new AzureStorageClient(storage.Settings, isMigrationActive: migrating);
            try
            {
                await client.InitializeMigrationAsync();
                await client.StopMigrationTokenRefreshAsync();
                // Fail one request for each service to check whether its SDK retries the write.
                foreach (Func<Task> write in CachedWrites(client))
                {
                    storage.Workload.Clear();
                    storage.FailNextWorkload = true;
                    if (migrating)
                    {
                        await Assert.ThrowsExceptionAsync<DurableTaskStorageException>(write);
                    }
                    else
                    {
                        await write();
                    }

                    // Migration writes make one attempt; normal writes retry and succeed.
                    Assert.AreEqual(migrating ? 1 : 2, storage.Workload.Count);
                    // Only migration requests should carry a SAS token and the 30-second server timeout.
                    foreach (Uri request in storage.Workload)
                    {
                        Dictionary<string, string> query = Query(request);
                        Assert.AreEqual(migrating, query.ContainsKey("sig"));
                        Assert.AreEqual(migrating, query.ContainsKey("timeout"));
                        if (migrating)
                        {
                            Assert.AreEqual("30", query["timeout"]);
                        }
                    }
                }
                // Normal clients should not create a migration gate or request delegation keys.
                if (!migrating)
                {
                    Assert.IsFalse(storage.GateExists);
                    Assert.AreEqual(0, storage.DelegationRequests);
                }
            }
            finally
            {
                await client.StopMigrationTokenRefreshAsync();
            }
        }

        static Func<Task>[] CachedWrites(AzureStorageClient client)
        {
            // Reuse the wrappers: recreating them would hide a stale cached-credential bug.
            Queue queue = client.GetQueueReference("testhub-control-00");
            Table table = client.GetTableReference("TestHubInstances");
            Blob blob = client.GetBlobReference("testhub-largemessages", "payload");
            return new Func<Task>[]
            {
                () => queue.AddMessageAsync("message", null),
                () => table.InsertEntityAsync(new TableEntity("instance", string.Empty)),
                () => blob.UploadTextAsync("payload"),
            };
        }

        static void AssertRequestsUseExpiry(IReadOnlyList<Uri> requests, DateTimeOffset expiry)
        {
            // Check that queue, table, and blob requests all use the expected SAS expiry.
            Assert.AreEqual(3, requests.Count);
            foreach (Uri request in requests)
            {
                Dictionary<string, string> query = Query(request);
                Assert.IsFalse(string.IsNullOrEmpty(query["sig"]));
                Assert.AreEqual(expiry, DateTimeOffset.Parse(query["se"], CultureInfo.InvariantCulture));
            }
        }

        static Dictionary<string, string> Query(Uri uri) => uri.Query.TrimStart('?')
            .Split(new[] { '&' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(part => part.Split(new[] { '=' }, 2))
            .ToDictionary(pair => pair[0], pair => Uri.UnescapeDataString(pair[1]));

        // Each response owns its in-memory body for the duration of the SDK pipeline.
        sealed class ResponseStream : MemoryStream
        {
            public ResponseStream(byte[] content) : base(content) { }
            protected override void Dispose(bool disposing) { }
        }

        // Use a dummy identity token; the fake transport handles every request locally.
        sealed class TestCredential : TokenCredential
        {
            public override AccessToken GetToken(TokenRequestContext context, CancellationToken cancellationToken) =>
                new AccessToken("test-token", DateTimeOffset.UtcNow.AddHours(1));

            public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext context, CancellationToken cancellationToken) =>
                new ValueTask<AccessToken>(this.GetToken(context, cancellationToken));
        }

        // Keep the actual SDK serialization, signing, authentication policies and retry pipelines.
        // Only Storage responses and its clock are simulated, including conditional gate writes.
        sealed class StorageResponses : HttpPipelineTransport, IDisposable
        {
            // Gate version, increased after each accepted write to produce a new ETag.
            int revision;

            // Whether the saved gate says migration is ending.
            bool ending;

            public StorageResponses()
            {
                // Use real SDK clients, but send their requests to this fake Storage service.
                HttpPipelineTransport transport = this;
                var credential = new TestCredential();
                var provider = new StorageAccountClientProvider(
                    StorageServiceClientProvider.ForBlob("account", credential, Options(new BlobClientOptions())),
                    StorageServiceClientProvider.ForQueue("account", credential, Options(new QueueClientOptions())),
                    StorageServiceClientProvider.ForTable("account", credential, Options(new TableClientOptions())));
                this.Settings = new AzureStorageOrchestrationServiceSettings { TaskHubName = "TestHub", StorageAccountClientProvider = provider };
                this.Control = new AzureStorageMigration(provider.Table.CreateClient(provider.Table.CreateOptions()), "TestHub");

                T Options<T>(T options) where T : ClientOptions
                {
                    options.Transport = transport;
                    // Allow one retry without waiting, so tests can see whether migration disables it.
                    options.Retry.MaxRetries = 1;
                    options.Retry.Mode = RetryMode.Fixed;
                    options.Retry.Delay = TimeSpan.Zero;
                    return options;
                }
            }

            // Settings that send all SDK requests through this fake Storage service.
            public AzureStorageOrchestrationServiceSettings Settings { get; }

            // Real gate helper used by tests to create the gate and start drain.
            public AzureStorageMigration Control { get; }

            // Clock returned in Storage responses; tests advance it to trigger renewal.
            public DateTimeOffset ServerTime { get; set; } = new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);

            // Last SAS access deadline saved in the gate.
            public DateTimeOffset Expiry { get; private set; }

            // Whether the gate row exists; tests can clear this to simulate its removal.
            public bool GateExists { get; set; }

            // Number of gate inserts and updates that committed successfully.
            public int GateWrites { get; private set; }

            // Number of gate updates rejected because their ETag was out of date.
            public int EtagConflicts { get; private set; }

            // Number of delegation key requests across blob, queue, and table services.
            public int DelegationRequests { get; private set; }

            // Return a server-busy error for gate reads while this is set.
            public bool FailGateReads { get; set; }

            // Fail the next data request, then clear the flag so a retry can succeed.
            public bool FailNextWorkload { get; set; }

            // Run once before checking whether the next gate insert can commit.
            public Func<Task>? BeforeInsert { get; set; }

            // Run once before checking the ETag on the next gate update.
            public Func<Task>? BeforeUpdate { get; set; }

            // Run once before returning the next delegation key response.
            public Func<Task>? BeforeKeyResponse { get; set; }

            // Data request URLs, including retries; excludes gate and delegation key requests.
            public List<Uri> Workload { get; } = new List<Uri>();

            // Current gate version in the format used by table responses and ETag checks.
            string ETag => $"W/\"{this.revision}\"";

            public async Task EndMigrationAsync()
            {
                // Start drain through the same conditional gate update used by production code.
                AzureStorageMigration.Gate gate = (await this.Control.ReadAsync(default))!;
                gate.IsMigrationEnding = true;
                Assert.IsTrue(await this.Control.TryUpdateAsync(gate, default));
            }

            async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            {
                Uri uri = request.RequestUri!;
                string body = request.Content == null ? string.Empty : await request.Content.ReadAsStringAsync();
                // Return a test delegation key for the requested service, after any injected drain.
                if (uri.Query.Contains("comp=userdelegationkey"))
                {
                    this.DelegationRequests++;
                    if (this.BeforeKeyResponse is Func<Task> hook)
                    {
                        this.BeforeKeyResponse = null;
                        await hook();
                    }
                    string service = uri.Host.Split('.')[1].Substring(0, 1);
                    return Respond(200, $"<UserDelegationKey><SignedOid>11111111-1111-1111-1111-111111111111</SignedOid><SignedTid>22222222-2222-2222-2222-222222222222</SignedTid><SignedStart>{this.ServerTime.AddMinutes(-15):yyyy-MM-ddTHH:mm:ssZ}</SignedStart><SignedExpiry>{this.ServerTime.AddHours(24):yyyy-MM-ddTHH:mm:ssZ}</SignedExpiry><SignedService>{service}</SignedService><SignedVersion>2025-07-05</SignedVersion><Value>{Convert.ToBase64String(new byte[32])}</Value></UserDelegationKey>", "application/xml");
                }
                if (uri.AbsolutePath == "/Tables")
                {
                    return Respond(201, "{\"TableName\":\"TestHubMigrationControl\"}");
                }
                if (uri.AbsolutePath.StartsWith("/TestHubMigrationControl", StringComparison.Ordinal))
                {
                    // Return the saved gate and ETag, or the read failure requested by the test.
                    if (request.Method == HttpMethod.Get)
                    {
                        if (this.FailGateReads)
                        {
                            return Error(503, "ServerBusy");
                        }
                        if (!this.GateExists)
                        {
                            return Error(404, "ResourceNotFound");
                        }
                        return Respond(200, new JObject
                        {
                            ["PartitionKey"] = "", ["RowKey"] = "", ["IsDraining"] = this.ending,
                            ["AccessExpiresAt@odata.type"] = "Edm.DateTime", ["AccessExpiresAt"] = this.Expiry.UtcDateTime,
                            ["odata.etag"] = this.ETag,
                        }.ToString());
                    }

                    // Run each race hook once, just before checking whether the write can commit.
                    bool inserting = request.Method == HttpMethod.Post;
                    Func<Task>? hook = inserting ? this.BeforeInsert : this.BeforeUpdate;
                    if (inserting) this.BeforeInsert = null;
                    else this.BeforeUpdate = null;
                    if (hook != null) await hook();
                    // Reject duplicate inserts and updates that used an old ETag.
                    if (inserting && this.GateExists) return Error(409, "EntityAlreadyExists");
                    if (!inserting && request.Headers.GetValues("If-Match").Single() != this.ETag)
                    {
                        this.EtagConflicts++;
                        return Error(412, "UpdateConditionNotSatisfied");
                    }
                    // Save an accepted gate write and give it a new ETag.
                    JObject row = JObject.Parse(body);
                    this.ending = row.Value<bool>("IsDraining");
                    this.Expiry = row["AccessExpiresAt"]!.ToObject<DateTimeOffset>();
                    this.GateExists = true;
                    this.revision++;
                    this.GateWrites++;
                    return Respond(204);
                }

                // Record data requests and check that SAS requests do not also send an identity token.
                this.Workload.Add(uri);
                if (Query(uri).ContainsKey("sig"))
                {
                    Assert.IsNull(request.Headers.Authorization, "SAS workload requests must not fall back to the source identity.");
                }
                // Fail only the next data request, so an SDK retry would succeed.
                if (this.FailNextWorkload)
                {
                    this.FailNextWorkload = false;
                    return Error(503, "ServerBusy");
                }
                // Return the success response each SDK expects for its write.
                if (uri.Host.Contains(".queue."))
                {
                    return Respond(201, $"<QueueMessagesList><QueueMessage><MessageId>id</MessageId><InsertionTime>{this.ServerTime:R}</InsertionTime><ExpirationTime>{this.ServerTime.AddDays(7):R}</ExpirationTime><PopReceipt>receipt</PopReceipt><TimeNextVisible>{this.ServerTime:R}</TimeNextVisible></QueueMessage></QueueMessagesList>", "application/xml");
                }
                return Respond(uri.Host.Contains(".blob.") ? 201 : 204);

                HttpResponseMessage Respond(int status, string content = "", string type = "application/json")
                {
                    var response = new HttpResponseMessage((HttpStatusCode)status)
                    {
                        RequestMessage = request,
                        Content = new StreamContent(new ResponseStream(System.Text.Encoding.UTF8.GetBytes(content))),
                    };
                    response.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue(type);
                    // Use the fake server clock and current gate version in response headers.
                    response.Headers.Date = this.ServerTime;
                    response.Headers.TryAddWithoutValidation("ETag", this.ETag);
                    return response;
                }

                HttpResponseMessage Error(int status, string code)
                {
                    // Match each service's error format so the SDK handles errors normally.
                    HttpResponseMessage response = uri.Host.Contains(".table.")
                        ? Respond(status, new JObject { ["odata.error"] = new JObject { ["code"] = code, ["message"] = new JObject { ["lang"] = "en-US", ["value"] = code } } }.ToString())
                        : Respond(status, $"<Error><Code>{code}</Code><Message>{code}</Message></Error>", "application/xml");
                    response.Headers.TryAddWithoutValidation("x-ms-error-code", code);
                    return response;
                }
            }

            public override Request CreateRequest() => HttpClientTransport.Shared.CreateRequest();
            public override void Process(HttpMessage message) => this.ProcessAsync(message).AsTask().GetAwaiter().GetResult();
            public override async ValueTask ProcessAsync(HttpMessage message)
            {
                // Copy the SDK request body and headers so the fake service sees what would be sent.
                using var request = new HttpRequestMessage(new HttpMethod(message.Request.Method.Method), message.Request.Uri.ToUri());
                if (message.Request.Content != null)
                {
                    using var body = new MemoryStream();
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
                // Handle the request locally and pass the response back through the SDK pipeline.
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
                // Keep a copy of the response after the temporary HTTP response is disposed.
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
    }
}
