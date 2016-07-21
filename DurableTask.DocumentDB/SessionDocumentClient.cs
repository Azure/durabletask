
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

namespace DurableTask.DocumentDb
{
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.Contracts;
    using System.IO;
    using System.Net;
    using System.Threading.Tasks;

    public class SessionDocumentClient
    {
        readonly string databaseName;
        readonly string collectionName;

        readonly DocumentClient documentClient;
        TaskCompletionSource<object> ensureOpenedTcs;

        public SessionDocumentClient(string documentDbEndpoint, 
            string documentDbKey, 
            string documentDbDatabase, 
            string documentDbCollection,
            bool cleanCollection = false)   // AFFANDAR : TODO : remove pls
        {
            Contract.Assert(!string.IsNullOrWhiteSpace(documentDbCollection));
            Contract.Assert(!string.IsNullOrWhiteSpace(documentDbKey));
            Contract.Assert(!string.IsNullOrWhiteSpace(documentDbDatabase));
            Contract.Assert(!string.IsNullOrWhiteSpace(documentDbCollection));

            this.collectionName = documentDbCollection;
            this.databaseName = documentDbDatabase;

            this.documentClient = new DocumentClient(new System.Uri(documentDbEndpoint), documentDbKey);
            ensureOpenedTcs = new TaskCompletionSource<object>();

            this.EnsureOpenedAsync(cleanCollection);
        }

        /// <summary>
        /// Get a list of all documents with sessionlock == (null or expired) and more than 0 elements in the orchestrator queue array
        /// or activity queue array dependong on forOrchestration flag
        /// Sort by insertion time in orchestrator/activity queue ASC
        /// pick count session docs in that array and put a lock on them with a TTL of 1 min
        /// </summary>
        /// <returns></returns>
        public async Task<SessionDocument> LockSessionDocumentAsync(bool forOrchestration)
        {
            await this.ensureOpenedTcs.Task;

            StoredProcedureResponse<SessionDocument> resp = 
                await this.documentClient.ExecuteStoredProcedureAsync<SessionDocument>(
                    UriFactory.CreateStoredProcedureUri(this.databaseName, this.collectionName, "lockSessionDocument"), 
                    forOrchestration        
                    );

           // resp.Response.Etag = (resp.ResponseHeaders.GetValues("etag"))[0];
            return resp.Response;
        }

        /// <summary>
        /// Unlock session document and replace in-place
        /// </summary>
        /// <param name="session"></param>
        /// <returns></returns>
        public async Task<SessionDocument> CompleteSessionDocumentAsync(SessionDocument session)
        {
            await this.ensureOpenedTcs.Task;
            StoredProcedureResponse<SessionDocument> resp =
                await this.documentClient.ExecuteStoredProcedureAsync<SessionDocument>(
                    UriFactory.CreateStoredProcedureUri(
                        this.databaseName, 
                        this.collectionName, 
                        "completeSessionDocument"),
                    session);

            return (SessionDocument)(dynamic)resp.Response;
        }

        // AFFANDAR : TODO : exception model.. should wrap docdb exceptions? maybe not because we are not hiding the docdb-ness here
        // AFFANDAR : TODO : move all calls to stored procs?
        public async Task CreateSessionDocumentAsync(SessionDocument sessionDoc)
        {
            await this.ensureOpenedTcs.Task;
            await this.documentClient.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri(databaseName, collectionName), sessionDoc);
        }

        public async Task DeleteSessionDocumentAsync(SessionDocument sessionDoc)
        {
            await this.ensureOpenedTcs.Task;
            await this.documentClient.DeleteDocumentAsync(UriFactory.CreateDocumentUri(databaseName, collectionName, sessionDoc.InstanceId));
        }

        public async Task<SessionDocument> FetchSessionDocumentAsync(string instanceId)
        {
            await this.ensureOpenedTcs.Task;

            StoredProcedureResponse<SessionDocument> resp =
                await this.documentClient.ExecuteStoredProcedureAsync<SessionDocument>(
                    UriFactory.CreateStoredProcedureUri(
                        this.databaseName, 
                        this.collectionName, 
                        "fetchSessionDocument"), 
                    instanceId
                    );

            return (SessionDocument)(dynamic)resp.Response;
        }

        async Task EnsureOpenedAsync(bool cleanCollection)
        {
            try
            {
                await this.CreateDatabaseIfNotExistsAsync(this.databaseName);

                // AFFANDAR : TODO : replace with if-not-exist
                if (!cleanCollection)
                {
                    await this.CreateDocumentCollectionIfNotExists(this.databaseName, this.collectionName);
                }
                else
                {
                    await this.CreateDocumentCollectionAsync(this.databaseName, this.collectionName);
                }

                await this.ProvisionStoredProcsAsync(this.databaseName, this.collectionName);
            }
            catch(Exception exception)
            {
                // AFFANDAR : TODO : isfatal
                this.ensureOpenedTcs.SetException(exception);
                return;
            }

            this.ensureOpenedTcs.SetResult(null);
        }

        async Task CreateDatabaseIfNotExistsAsync(string databaseName)
        {
            try
            {
                await this.documentClient.ReadDatabaseAsync(UriFactory.CreateDatabaseUri(databaseName));
            }
            catch (DocumentClientException de)
            {
                // If the database does not exist, create a new database
                if (de.StatusCode == HttpStatusCode.NotFound)
                {
                    await this.documentClient.CreateDatabaseAsync(new Database { Id = databaseName });
                }
                else
                {
                    throw;
                }
            }
        }

        async Task ProvisionStoredProcsAsync(string databaseName, string collectionName)
        {
            // AFFANDAR : TODO : read from sp script file
            //          for now just inline it

            string lockSessionDocument = File.ReadAllText(@".\scripts\lockSessionDocument.js");
            string completeSessionDocument = File.ReadAllText(@".\scripts\completeSessionDocument.js");
            string updateSessionDocumentQueue = File.ReadAllText(@".\scripts\updateSessionDocumentQueue.js");
            string fetchSessionDocument = File.ReadAllText(@".\scripts\fetchSessionDocument.js");


            await this.CreateStoredProcAsync(databaseName, collectionName, "lockSessionDocument", lockSessionDocument);
            await this.CreateStoredProcAsync(databaseName, collectionName, "completeSessionDocument", completeSessionDocument);
            await this.CreateStoredProcAsync(databaseName, collectionName, "updateSessionDocumentQueue", updateSessionDocumentQueue);
            await this.CreateStoredProcAsync(databaseName, collectionName, "fetchSessionDocument", fetchSessionDocument);
        }

        // AFFANDAR : TODO : figure out all the creation options
        async Task CreateDocumentCollectionIfNotExists(string databaseName, string collectionName)
        {
            try
            {
                await this.documentClient.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(databaseName, collectionName));
            }
            catch (DocumentClientException de)
            {
                // If the document collection does not exist, create a new collection
                if (de.StatusCode == HttpStatusCode.NotFound)
                {
                    DocumentCollection collectionInfo = new DocumentCollection();
                    collectionInfo.Id = collectionName;

                    // Configure collections for maximum query flexibility including string range queries.
                    collectionInfo.IndexingPolicy = new IndexingPolicy(new RangeIndex(DataType.String) { Precision = -1 });

                    // Here we create a collection with 400 RU/s.
                    await this.documentClient.CreateDocumentCollectionAsync(
                        UriFactory.CreateDatabaseUri(databaseName),
                        collectionInfo,
                        new RequestOptions { OfferThroughput = 400 });
                }
                else
                {
                    throw;
                }
            }
        }

        // AFFANDAR : TODO : figure out all the creation options
        async Task CreateDocumentCollectionAsync(string databaseName, string collectionName)
        {
            try
            {
                await this.documentClient.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(databaseName, collectionName));

                // delete and then recreate
                await this.documentClient.DeleteDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(databaseName, collectionName));
                await CreateNewDocumentCollectionAsync(databaseName, collectionName);
            }
            catch (DocumentClientException de)
            {
                // If the document collection does not exist, create a new collection
                if (de.StatusCode == HttpStatusCode.NotFound)
                {
                    await CreateNewDocumentCollectionAsync(databaseName, collectionName);
                }
                else
                {
                    throw;
                }
            }
        }

        async Task CreateNewDocumentCollectionAsync(string databaseName, string collectionName)
        {
            DocumentCollection collectionInfo = new DocumentCollection();
            collectionInfo.Id = collectionName;

            // Configure collections for maximum query flexibility including string range queries.
            collectionInfo.IndexingPolicy = new IndexingPolicy(new RangeIndex(DataType.String) { Precision = -1 });

            // Here we create a collection with 400 RU/s.
            await this.documentClient.CreateDocumentCollectionAsync(
                UriFactory.CreateDatabaseUri(databaseName),
                collectionInfo,
                new RequestOptions { OfferThroughput = 400 });
        }

        async Task CreateStoredProcAsync(
            string databaseName,
            string collectionName,
            string storedProcId,
            string storedProcText)
        {
            var sproc = new StoredProcedure
            {
                Id = storedProcId,
                Body = storedProcText
            };

            try
            {
                await this.documentClient.ReadStoredProcedureAsync(
                    UriFactory.CreateStoredProcedureUri(databaseName, collectionName, storedProcId));

                await this.documentClient.ReplaceStoredProcedureAsync(sproc);
            }
            catch (DocumentClientException de)
            {
                // If the document collection does not exist, create a new collection
                if (de.StatusCode == HttpStatusCode.NotFound)
                {
                    await this.documentClient.CreateStoredProcedureAsync(
                        UriFactory.CreateDocumentCollectionUri(databaseName, collectionName), sproc);
                }
                else
                {
                    throw;
                }
            }
        }
    }
}