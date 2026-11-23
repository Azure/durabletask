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
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Core;
    using Azure.Core.Pipeline;
    using Azure.Data.Tables;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json.Linq;

    [TestClass]
    public class AzureStorageMigrationTests
    {
        static readonly DateTimeOffset ServerTime = new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);

        [TestMethod]
        public async Task Read_UsesAttemptRowAndStorageClock()
        {
            // 1. Return an existing attempt with the Storage response time.
            Guid id = Guid.NewGuid();
            JObject row = Row(id);
            using TableTransport client = new TableTransport(request =>
            {
                Assert.AreEqual(HttpMethod.Get, request.Method);
                Assert.AreEqual(Path(id), Uri.UnescapeDataString(request.RequestUri!.AbsolutePath));
                return Response(HttpStatusCode.OK, row);
            });
            AzureStorageMigration migration = CreateMigration(client);

            // 2. Keep the stored values, ETag, and server clock.
            AzureStorageMigration.Gate gate = (await migration.ReadAsync(default, id))!;
            Assert.IsNotNull(gate);
            Assert.AreEqual(id, gate.MigrationId);
            Assert.IsFalse(gate.IsMigrationEnding);
            Assert.AreEqual(ServerTime.AddSeconds(15), gate.AccessExpiresAt);
            Assert.AreEqual(new ETag("v1"), gate.ETag);
            Assert.AreEqual(ServerTime, gate.ServerTime);
        }

        [DataTestMethod]
        [DataRow("ResourceNotFound")]
        [DataRow("TableNotFound")]
        public async Task Read_MissingRowOrTableDoesNotCreateIt(string code)
        {
            // 1. Reject every request with a missing-row or missing-table response.
            int reads = 0;
            using TableTransport client = new TableTransport(request =>
            {
                Assert.AreEqual(HttpMethod.Get, request.Method);
                reads++;
                return Error(HttpStatusCode.NotFound, code);
            });

            // 2. Report absence without inserting a replacement row or table.
            Assert.IsNull(await CreateMigration(client).ReadAsync(default, Guid.NewGuid()));
            Assert.AreEqual(1, reads);
        }

        [DataTestMethod]
        [DataRow(403, "AuthorizationPermissionMismatch")]
        [DataRow(404, "OtherNotFound")]
        [DataRow(503, "ServerBusy")]
        public async Task Read_PropagatesOtherFailures(int status, string code)
        {
            // 1. Return a failure that does not mean the control row is missing.
            using TableTransport client = new TableTransport(_ => Error((HttpStatusCode)status, code));

            // 2. Preserve the failure for the caller.
            RequestFailedException error = await Assert.ThrowsExceptionAsync<RequestFailedException>(
                () => CreateMigration(client).ReadAsync(default, Guid.NewGuid()));
            Assert.AreEqual(status, error.Status);
            Assert.AreEqual(code, error.ErrorCode);
        }

        [DataTestMethod]
        [DataRow("MigrationId")]
        [DataRow("IsDraining")]
        [DataRow("AccessExpiresAt")]
        [DataRow("Date")]
        public async Task Read_RejectsMissingRequiredValues(string missing)
        {
            // 1. Omit one required property or the Storage Date header.
            Guid id = Guid.NewGuid();
            JObject row = Row(id);
            row.Remove(missing);
            row.Remove(missing + "@odata.type");
            using TableTransport client = new TableTransport(_ => Response(HttpStatusCode.OK, row, missing != "Date"));

            // 2. Do not invent a drain state, access deadline, or server time.
            InvalidOperationException error = await Assert.ThrowsExceptionAsync<InvalidOperationException>(
                () => CreateMigration(client).ReadAsync(default, id));
            StringAssert.Contains(error.Message, missing);
        }

        [TestMethod]
        public async Task Update_StaleRenewalCannotOverwriteDrain()
        {
            // 1. Two hosts read the same open attempt.
            Guid id = Guid.NewGuid();
            Dictionary<Guid, JObject> rows = new Dictionary<Guid, JObject> { [id] = Row(id) };
            using TableTransport client = new TableTransport(request => Serve(rows, request));
            AzureStorageMigration migration = CreateMigration(client);
            AzureStorageMigration.Gate renewal = (await migration.ReadAsync(default, id))!;
            AzureStorageMigration.Gate drain = (await migration.ReadAsync(default, id))!;

            // 2. Close the gate before the other host tries to extend its deadline.
            drain.IsMigrationEnding = true;
            Assert.IsTrue(await migration.TryUpdateAsync(drain, default));
            renewal.AccessExpiresAt = ServerTime.AddMinutes(1);
            Assert.IsFalse(await migration.TryUpdateAsync(renewal, default));

            // 3. The stale write changes neither the drain flag nor the deadline.
            AzureStorageMigration.Gate saved = (await migration.ReadAsync(default, id))!;
            Assert.IsTrue(saved.IsMigrationEnding);
            Assert.AreEqual(ServerTime.AddSeconds(15), saved.AccessExpiresAt);
            Assert.AreNotEqual(renewal.ETag, saved.ETag);
        }

        [TestMethod]
        public async Task Update_NewAttemptLeavesOldAttemptClosed()
        {
            // 1. Keep a closed attempt alongside a new open attempt.
            Guid oldId = Guid.NewGuid();
            Guid newId = Guid.NewGuid();
            Dictionary<Guid, JObject> rows = new Dictionary<Guid, JObject>
            {
                [oldId] = Row(oldId, ending: true),
                [newId] = Row(newId),
            };
            using TableTransport client = new TableTransport(request => Serve(rows, request));
            AzureStorageMigration migration = CreateMigration(client);
            AzureStorageMigration.Gate original = (await migration.ReadAsync(default, oldId))!;

            // 2. Renew only the new attempt.
            AzureStorageMigration.Gate next = (await migration.ReadAsync(default, newId))!;
            next.AccessExpiresAt = ServerTime.AddSeconds(30);
            Assert.IsTrue(await migration.TryUpdateAsync(next, default));

            // 3. The old attempt keeps its closed state, deadline, and ETag.
            AzureStorageMigration.Gate retained = (await migration.ReadAsync(default, oldId))!;
            Assert.AreEqual(oldId, retained.MigrationId);
            Assert.IsTrue(retained.IsMigrationEnding);
            Assert.AreEqual(original.AccessExpiresAt, retained.AccessExpiresAt);
            Assert.AreEqual(original.ETag, retained.ETag);
            AzureStorageMigration.Gate updated = (await migration.ReadAsync(default, newId))!;
            Assert.AreEqual(newId, updated.MigrationId);
            Assert.IsFalse(updated.IsMigrationEnding);
            Assert.AreEqual(next.AccessExpiresAt, updated.AccessExpiresAt);
            Assert.AreEqual(2, rows.Count);
        }

        [DataTestMethod]
        [DataRow(404, "ResourceNotFound")]
        [DataRow(403, "AuthorizationPermissionMismatch")]
        [DataRow(503, "ServerBusy")]
        public async Task Update_PropagatesFailuresOtherThanEtagConflict(int status, string code)
        {
            // 1. Allow the read, then fail the conditional update.
            Guid id = Guid.NewGuid();
            using TableTransport client = new TableTransport(request => request.Method == HttpMethod.Get
                ? Response(HttpStatusCode.OK, Row(id)) : Error((HttpStatusCode)status, code));
            AzureStorageMigration migration = CreateMigration(client);
            AzureStorageMigration.Gate gate = (await migration.ReadAsync(default, id))!;

            // 2. Missing rows and service errors must not look like a competing update.
            RequestFailedException error = await Assert.ThrowsExceptionAsync<RequestFailedException>(
                () => migration.TryUpdateAsync(gate, default));
            Assert.AreEqual(status, error.Status);
            Assert.AreEqual(code, error.ErrorCode);
        }

        static AzureStorageMigration CreateMigration(TableTransport client)
        {
            TableClientOptions options = new TableClientOptions { Transport = client };
            options.Retry.MaxRetries = 0;
            TableServiceClient service = new TableServiceClient(new Uri("https://account.table.core.windows.net"), new AzureSasCredential("sig=test"), options);
            return new AzureStorageMigration(service, "TestHub");
        }

        static string Path(Guid id) => $"/TestHubMigrationCtrl(PartitionKey='',RowKey='{id:D}')";

        static JObject Row(Guid id, bool ending = false) => new JObject
        {
            ["PartitionKey"] = string.Empty,
            ["RowKey"] = id.ToString("D"),
            ["MigrationId@odata.type"] = "Edm.Guid",
            ["MigrationId"] = id,
            ["IsDraining"] = ending,
            ["AccessExpiresAt@odata.type"] = "Edm.DateTime",
            ["AccessExpiresAt"] = ServerTime.AddSeconds(15),
            ["odata.etag"] = "v1",
        };

        static HttpResponseMessage Serve(Dictionary<Guid, JObject> rows, HttpRequestMessage request)
        {
            string path = Uri.UnescapeDataString(request.RequestUri!.AbsolutePath);
            Guid id = rows.Keys.Single(candidate => Path(candidate) == path);
            JObject saved = rows[id];
            if (request.Method == HttpMethod.Get)
            {
                return Response(HttpStatusCode.OK, saved);
            }

            Assert.AreEqual(HttpMethod.Put, request.Method);
            if (request.Headers.GetValues("If-Match").Single() != saved.Value<string>("odata.etag"))
            {
                return Error(HttpStatusCode.PreconditionFailed, "UpdateConditionNotSatisfied");
            }

            JObject updated = JObject.Parse(request.Content!.ReadAsStringAsync().GetAwaiter().GetResult());
            Assert.AreEqual(string.Empty, updated.Value<string>("PartitionKey"));
            Assert.AreEqual(id.ToString("D"), updated.Value<string>("RowKey"));
            Assert.AreEqual("Edm.Guid", updated.Value<string>("MigrationId@odata.type"));
            Assert.AreEqual(id, updated["MigrationId"]!.ToObject<Guid>());
            updated["odata.etag"] = Guid.NewGuid().ToString("N");
            rows[id] = updated;
            return Response(HttpStatusCode.NoContent);
        }

        static HttpResponseMessage Error(HttpStatusCode status, string code)
        {
            HttpResponseMessage response = Response(status, new JObject
            {
                ["odata.error"] = new JObject { ["code"] = code, ["message"] = new JObject { ["lang"] = "en-US", ["value"] = code } },
            });
            response.Headers.TryAddWithoutValidation("x-ms-error-code", code);
            return response;
        }

        static HttpResponseMessage Response(HttpStatusCode status, JObject? body = null, bool includeDate = true)
        {
            HttpResponseMessage response = new HttpResponseMessage(status)
            {
                Content = new StreamContent(new ResponseStream(Encoding.UTF8.GetBytes(body?.ToString() ?? string.Empty))),
            };
            response.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");
            if (includeDate)
            {
                response.Headers.Date = ServerTime;
            }
            return response;
        }

        // Keep the fake response body available while the SDK reads it.
        sealed class ResponseStream : MemoryStream
        {
            public ResponseStream(byte[] content) : base(content) { }
            protected override void Dispose(bool disposing) { }
        }

        sealed class TableTransport : HttpPipelineTransport, IDisposable
        {
            readonly Func<HttpRequestMessage, HttpResponseMessage> respond;

            public TableTransport(Func<HttpRequestMessage, HttpResponseMessage> respond) => this.respond = respond;

            public override Request CreateRequest() => HttpClientTransport.Shared.CreateRequest();

            public override void Process(HttpMessage message) => this.ProcessAsync(message).AsTask().GetAwaiter().GetResult();

            public override async ValueTask ProcessAsync(HttpMessage message)
            {
                // Pass the SDK's request to the local Table handler.
                message.CancellationToken.ThrowIfCancellationRequested();
                using HttpRequestMessage request = new HttpRequestMessage(new HttpMethod(message.Request.Method.Method), message.Request.Uri.ToUri());
                if (message.Request.Content != null)
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
                using HttpResponseMessage response = this.respond(request);
                message.Response = new TableResponse(response, await response.Content.ReadAsByteArrayAsync());
            }

            public void Dispose() { }
        }

        sealed class TableResponse : Response
        {
            readonly Dictionary<string, string> headers;

            public TableResponse(HttpResponseMessage response, byte[] body)
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
    }
}
