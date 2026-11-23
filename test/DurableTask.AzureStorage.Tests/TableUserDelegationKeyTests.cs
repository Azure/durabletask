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
    using System.Xml.Linq;
    using Azure;
    using Azure.Core;
    using Azure.Core.Pipeline;
    using Azure.Data.Tables;
    using DurableTask.AzureStorage.Storage;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class TableUserDelegationKeyTests
    {
        const string KeyValue = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
        const string KeyStart = "2030-01-01T00:00:00Z";
        const string KeyExpiry = "2030-01-02T00:15:00Z";
        const string ObjectId = "11111111-1111-1111-1111-111111111111";
        const string TenantId = "22222222-2222-2222-2222-222222222222";
        static readonly Uri Endpoint = new Uri("https://account.table.core.windows.net/");
        static readonly DateTimeOffset ServerTime = new DateTimeOffset(2030, 1, 1, 8, 15, 0, TimeSpan.FromHours(8));

        [TestMethod]
        public async Task Get_UsesStorageTimeAndAnIndependentAuthenticatedPipeline()
        {
            // 1. Inspect the authenticated request without contacting Storage.
            int requests = 0;
            using HttpClient http = new HttpClient(new TestHandler(async (request, token) =>
            {
                requests++;
                Assert.AreEqual(HttpMethod.Post, request.Method);
                Assert.AreEqual(new Uri(Endpoint, "?restype=service&comp=userdelegationkey"), request.RequestUri);
                Assert.AreEqual("Bearer", request.Headers.Authorization!.Scheme);
                Assert.AreEqual("unit-test-token", request.Headers.Authorization.Parameter);
                Assert.AreEqual("2025-07-05", request.Headers.GetValues("x-ms-version").Single());
                Assert.AreEqual("application/xml", request.Content!.Headers.ContentType!.MediaType);
                XElement body = XElement.Parse(await request.Content.ReadAsStringAsync());
                Assert.AreEqual("KeyInfo", body.Name.LocalName);
                Assert.AreEqual(KeyStart, (string?)body.Element("Start"));
                Assert.AreEqual(KeyExpiry, (string?)body.Element("Expiry"));
                return KeyResponse();
            }));
            TableClientOptions options = Options(http);
            options.AddPolicy(new RejectWorkloadRequestsPolicy(), HttpPipelinePosition.PerCall);
            TestCredential credential = new TestCredential();

            // 2. Key acquisition must not run policies that require a workload SAS.
            TableUserDelegationKey key = await TableUserDelegationKey.GetAsync(Endpoint, credential, options, ServerTime, default);
            Assert.AreEqual(new DateTimeOffset(2030, 1, 2, 0, 15, 0, TimeSpan.Zero), key.ExpiresOn);
            Assert.AreEqual(1, requests);
            CollectionAssert.AreEqual(new[] { "https://storage.azure.com/.default" }, credential.Scopes);
        }

        [DataTestMethod]
        [DataRow(null, "https://storage.azure.com/.default")]
        [DataRow("https://storage.azure.com/", "https://storage.azure.com/.default")]
        [DataRow("https://storage.azure.us/", "https://storage.azure.us/.default")]
        [DataRow("https://storage.azure.cn/", "https://storage.azure.cn/.default")]
        [DataRow("https://custom.storage.example/", "https://custom.storage.example/.default")]
        [DataRow("https://custom.storage.example/.default", "https://custom.storage.example/.default")]
        public async Task Get_UsesTheConfiguredAudience(string? audience, string expectedScope)
        {
            // 1. Supply a cloud or custom token audience.
            using HttpClient http = new HttpClient(new TestHandler((request, token) => Task.FromResult(KeyResponse())));
            TableClientOptions options = Options(http);
            if (audience != null)
            {
                options.Audience = new TableAudience(audience);
            }
            TestCredential credential = new TestCredential();

            // 2. Request the token for that audience exactly once.
            await TableUserDelegationKey.GetAsync(Endpoint, credential, options, ServerTime, default);
            CollectionAssert.AreEqual(new[] { expectedScope }, credential.Scopes);
            Assert.AreEqual(1, credential.Requests);
        }

        [DataTestMethod]
        [DataRow(503, 0, 1, false)]
        [DataRow(503, 1, 2, true)]
        [DataRow(403, 1, 1, false)]
        public async Task Get_HonorsRetryOptionsAndPropagatesFailures(int status, int maxRetries, int expectedRequests, bool succeeds)
        {
            // 1. Fail the first request and allow one later request to succeed.
            int requests = 0;
            using HttpClient http = new HttpClient(new TestHandler((request, token) =>
            {
                requests++;
                HttpResponseMessage response = requests == 1
                    ? ErrorResponse(status, "<Error><Code>TestFailure</Code><Message>Storage refused the test request.</Message></Error>")
                    : KeyResponse();
                return Task.FromResult(response);
            }));
            TableClientOptions options = Options(http);
            options.Retry.Mode = RetryMode.Fixed;
            options.Retry.Delay = TimeSpan.Zero;
            options.Retry.MaxDelay = TimeSpan.Zero;
            options.Retry.MaxRetries = maxRetries;

            // 2. Retry only when both the response and the configured policy allow it.
            if (succeeds)
            {
                await TableUserDelegationKey.GetAsync(Endpoint, new TestCredential(), options, ServerTime, default);
            }
            else
            {
                RequestFailedException failure = await Assert.ThrowsExceptionAsync<RequestFailedException>(() =>
                    TableUserDelegationKey.GetAsync(Endpoint, new TestCredential(), options, ServerTime, default));
                Assert.AreEqual(status, failure.Status);
                Assert.AreEqual("TestFailure", failure.ErrorCode);
                StringAssert.Contains(failure.Message, "Storage refused the test request.");
                StringAssert.Contains(failure.Message, "Status: " + status);
                StringAssert.Contains(failure.Message, "<Code>TestFailure</Code>");
                Response response = failure.GetRawResponse()!;
                Assert.IsNotNull(response);
                Assert.IsTrue(response.Headers.TryGetValue("x-ms-request-id", out string? requestId));
                Assert.AreEqual("test-request-id", requestId);
            }
            Assert.AreEqual(expectedRequests, requests);
        }

        [DataTestMethod]
        [DataRow("<Error><Code>TestFailure", null)]
        [DataRow("<Error><Message>No code was returned.</Message></Error>", null)]
        [DataRow("Service unavailable", null)]
        [DataRow("", null)]
        [DataRow("{\"error\":{\"code\":\"JsonFailure\",\"message\":\"Storage refused the JSON request.\"}}", "JsonFailure")]
        public async Task Get_PreservesFailuresForOtherResponseBodies(string body, string? expectedCode)
        {
            // 1. Return a body that is not a complete XML error.
            using HttpClient http = new HttpClient(new TestHandler((request, token) =>
                Task.FromResult(ErrorResponse(403, body))));

            // 2. Keep the HTTP failure and any error code the SDK can parse.
            RequestFailedException failure = await Assert.ThrowsExceptionAsync<RequestFailedException>(() =>
                TableUserDelegationKey.GetAsync(Endpoint, new TestCredential(), Options(http), ServerTime, default));
            Assert.AreEqual(403, failure.Status);
            Assert.AreEqual(expectedCode, failure.ErrorCode);
            if (body.Length != 0)
            {
                StringAssert.Contains(failure.Message, body);
            }
            Response response = failure.GetRawResponse()!;
            Assert.IsNotNull(response);
            Assert.AreEqual(403, response.Status);
            Assert.IsTrue(response.Headers.TryGetValue("x-ms-request-id", out string? requestId));
            Assert.AreEqual("test-request-id", requestId);
        }

        [TestMethod]
        [Timeout(10000)]
        public async Task Get_CancelsAnOutstandingRequest()
        {
            // 1. Hold the response until the caller cancels.
            TaskCompletionSource<bool> started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            using HttpClient http = new HttpClient(new TestHandler(async (request, token) =>
            {
                started.SetResult(true);
                await Task.Delay(Timeout.Infinite, token);
                return KeyResponse();
            }));
            using CancellationTokenSource cancellation = new CancellationTokenSource();
            Task<TableUserDelegationKey> pending = TableUserDelegationKey.GetAsync(
                Endpoint, new TestCredential(), Options(http), ServerTime, cancellation.Token);
            await started.Task;

            // 2. Cancellation must stop the request instead of returning a key.
            cancellation.Cancel();
            try
            {
                await pending;
                Assert.Fail("Key acquisition should have been canceled.");
            }
            catch (OperationCanceledException)
            {
                Assert.IsTrue(cancellation.IsCancellationRequested);
            }
        }

        [DataTestMethod]
        [DataRow("SignedOid")]
        [DataRow("SignedTid")]
        [DataRow("SignedStart")]
        [DataRow("SignedExpiry")]
        [DataRow("SignedService")]
        [DataRow("SignedVersion")]
        [DataRow("Value")]
        public async Task Get_RejectsMissingKeyFields(string missing)
        {
            // 1. Omit a required field from an otherwise valid response.
            XElement body = KeyBody();
            body.Element(missing)!.Remove();
            using HttpClient http = new HttpClient(new TestHandler((request, token) => Task.FromResult(KeyResponse(body))));

            // 2. Reject the incomplete key before it can sign a SAS.
            InvalidOperationException failure = await Assert.ThrowsExceptionAsync<InvalidOperationException>(() =>
                TableUserDelegationKey.GetAsync(Endpoint, new TestCredential(), Options(http), ServerTime, default));
            StringAssert.Contains(failure.Message, missing);
        }

        [DataTestMethod]
        [DataRow("b")]
        [DataRow("q")]
        public async Task Get_RejectsKeysForOtherServices(string service)
        {
            // 1. Return a valid key for a different Storage service.
            XElement body = KeyBody();
            body.Element("SignedService")!.Value = service;
            using HttpClient http = new HttpClient(new TestHandler((request, token) => Task.FromResult(KeyResponse(body))));

            // 2. Never use a Blob or Queue key to sign Table requests.
            InvalidOperationException failure = await Assert.ThrowsExceptionAsync<InvalidOperationException>(() =>
                TableUserDelegationKey.GetAsync(Endpoint, new TestCredential(), Options(http), ServerTime, default));
            StringAssert.Contains(failure.Message, "Table user delegation key");
        }

        [DataTestMethod]
        [DataRow("account", "HistoryTable", 15, "20YN1Rzzdm1GF9MrfJaD0Zn2RoIvejbHZbM/L/7tWD8=")]
        [DataRow("account", "historytable", 15, "20YN1Rzzdm1GF9MrfJaD0Zn2RoIvejbHZbM/L/7tWD8=")]
        [DataRow("otheraccount", "HistoryTable", 15, "kR3sfLgvjLyPkp5NOSENr3DbZl9+rj4eYHjRyyROK/Q=")]
        [DataRow("account", "HistoryTable", 30, "OKzkOhSjSgdspP7c/WzkSbHWlpUh+Kj+a5A+ZQ0Ods8=")]
        public async Task Sign_MatchesTheTableServiceSignature(string account, string table, int seconds, string expectedSignature)
        {
            // 1. Load a fixed test key and choose a short SAS expiry.
            using HttpClient http = new HttpClient(new TestHandler((request, token) => Task.FromResult(KeyResponse())));
            TableUserDelegationKey key = await TableUserDelegationKey.GetAsync(
                Endpoint, new TestCredential(), Options(http), ServerTime, default);
            DateTimeOffset expiresOn = ServerTime.AddSeconds(seconds);

            // 2. Check the REST signature, including empty fields and lowercase table name.
            Dictionary<string, string> query = key.Sign(account, table, expiresOn).Split('&')
                .Select(part => part.Split(new[] { '=' }, 2))
                .ToDictionary(parts => parts[0], parts => Uri.UnescapeDataString(parts[1]));
            Assert.AreEqual(expectedSignature, query["sig"]);
            Assert.AreEqual(table, query["tn"]);
            Assert.AreEqual("raud", query["sp"]);
            Assert.AreEqual("https", query["spr"]);
            Assert.AreEqual("2025-07-05", query["sv"]);
            Assert.AreEqual(KeyStart, query["st"]);
            Assert.AreEqual(expiresOn.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ssZ", System.Globalization.CultureInfo.InvariantCulture), query["se"]);
            Assert.AreEqual(ObjectId, query["skoid"]);
            Assert.AreEqual(TenantId, query["sktid"]);
            Assert.AreEqual(KeyStart, query["skt"]);
            Assert.AreEqual(KeyExpiry, query["ske"]);
            Assert.AreEqual("t", query["sks"]);
            Assert.AreEqual("2025-07-05", query["skv"]);
            Assert.AreEqual(13, query.Count);
        }

        static TableClientOptions Options(HttpClient http)
        {
            TableClientOptions options = new TableClientOptions { Transport = new HttpClientTransport(http) };
            options.Retry.MaxRetries = 0;
            return options;
        }

        static XElement KeyBody() => new XElement("UserDelegationKey",
            new XElement("SignedOid", ObjectId),
            new XElement("SignedTid", TenantId),
            new XElement("SignedStart", KeyStart),
            new XElement("SignedExpiry", KeyExpiry),
            new XElement("SignedService", "t"),
            new XElement("SignedVersion", "2025-07-05"),
            new XElement("Value", KeyValue));

        static HttpResponseMessage KeyResponse(XElement? body = null) => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent((body ?? KeyBody()).ToString(SaveOptions.DisableFormatting), Encoding.UTF8, "application/xml"),
        };

        static HttpResponseMessage ErrorResponse(int status, string body)
        {
            // The HTTP pipeline owns and disposes the returned response.
            HttpResponseMessage response = new HttpResponseMessage((HttpStatusCode)status)
            {
                Content = new StringContent(body, Encoding.UTF8, "application/xml"),
            };
            response.Headers.Add("x-ms-request-id", "test-request-id");
            return response;
        }

        sealed class TestCredential : TokenCredential
        {
            public string[] Scopes { get; private set; } = Array.Empty<string>();
            public int Requests { get; private set; }

            public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                this.Scopes = requestContext.Scopes;
                this.Requests++;
                return new AccessToken("unit-test-token", DateTimeOffset.UtcNow.AddHours(1));
            }

            public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
                new ValueTask<AccessToken>(this.GetToken(requestContext, cancellationToken));
        }

        sealed class RejectWorkloadRequestsPolicy : HttpPipelineSynchronousPolicy
        {
            public override void OnSendingRequest(HttpMessage message) =>
                throw new InvalidOperationException("Key acquisition must not use workload policies.");
        }

        sealed class TestHandler : HttpMessageHandler
        {
            readonly Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> respond;

            public TestHandler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> respond) => this.respond = respond;

            protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
                this.respond(request, cancellationToken);
        }
    }
}
