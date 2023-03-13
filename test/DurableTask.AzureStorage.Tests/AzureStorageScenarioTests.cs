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

namespace DurableTask.AzureStorage.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;
    using DurableTask.Core.Entities;
    using DurableTask.Core.Entities.OperationFormat;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Core.Serializing;
    using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Utility;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.Table;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    [TestClass]
    public class AzureStorageScenarioTests
    {
        public static readonly TimeSpan StandardTimeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(30);

        /// <summary>
        /// End-to-end test which validates a simple orchestrator function which doesn't call any activity functions.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task HelloWorldOrchestration_Inline(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloInline), "World");
                var status = await client.WaitForCompletionAsync(StandardTimeout);

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual("World", JToken.Parse(status?.Input));
                Assert.AreEqual("Hello, World!", JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which runs a simple orchestrator function that calls a single activity function.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task HelloWorldOrchestration_Activity(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloWithActivity), "World");
                var status = await client.WaitForCompletionAsync(StandardTimeout);

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual("World", JToken.Parse(status?.Input));
                Assert.AreEqual("Hello, World!", JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates function chaining by implementing a naive factorial function orchestration.
        /// </summary>
        [TestMethod]
        public async Task SequentialOrchestration()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Factorial), 10);
                var status = await client.WaitForCompletionAsync(StandardTimeout);

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(10, JToken.Parse(status?.Input));
                Assert.AreEqual(3628800, JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates function chaining by implementing a naive factorial function orchestration.
        /// </summary>
        [TestMethod]
        public async Task SequentialOrchestrationNoReplay()
        {
            // Enable extended sesisons to ensure that the orchestration never gets replayed
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.FactorialNoReplay), 10);
                var status = await client.WaitForCompletionAsync(StandardTimeout);

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(10, JToken.Parse(status?.Input));
                Assert.AreEqual(3628800, JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task ParentOfSequentialOrchestration()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.ParentOfFactorial), 10);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(10, JToken.Parse(status?.Input));
                Assert.AreEqual(3628800, JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which runs a slow orchestrator that causes work item renewal
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task LongRunningOrchestrator(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(
                enableExtendedSessions,
                modifySettingsAction: (AzureStorageOrchestrationServiceSettings settings) =>
                {
                    // set a short timeout so we can test that the renewal works
                    settings.ControlQueueVisibilityTimeout = TimeSpan.FromSeconds(10);
                }))
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.LongRunningOrchestrator), "0");
                var status = await client.WaitForCompletionAsync(StandardTimeout);

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual("ok", JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }


        [TestMethod]
        public async Task GetAllOrchestrationStatuses()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                // Execute the orchestrator twice. Orchestrator will be replied. However instances might be two.
                await host.StartAsync();
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloInline), "world one");
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                client = await host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloInline), "world two");
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                var results = await host.GetAllOrchestrationInstancesAsync();
                Assert.AreEqual(2, results.Count);
                Assert.IsNotNull(results.SingleOrDefault(r => r.Output == "\"Hello, world one!\""));
                Assert.IsNotNull(results.SingleOrDefault(r => r.Output == "\"Hello, world two!\""));

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task GetPaginatedStatuses()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                // Execute the orchestrator twice. Orchestrator will be replied. However instances might be two.
                await host.StartAsync();
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloInline), "world one");
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                client = await host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloInline), "world two");
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                DurableStatusQueryResult queryResult = await host.service.GetOrchestrationStateAsync(
                    new OrchestrationInstanceStatusQueryCondition(),
                    1,
                    null);
                Assert.AreEqual(1, queryResult.OrchestrationState.Count());
                Assert.IsNotNull(queryResult.ContinuationToken);
                queryResult = await host.service.GetOrchestrationStateAsync(
                    new OrchestrationInstanceStatusQueryCondition(),
                    1,
                    queryResult.ContinuationToken);
                Assert.AreEqual(1, queryResult.OrchestrationState.Count());
                Assert.IsNull(queryResult.ContinuationToken);
                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task GetInstanceIdsByPrefix()
        {
            using TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false);
            string instanceIdPrefixGuid = "0abb6ebb-d712-453a-97c4-6c7c1f78f49f";

            string[] instanceIds = new[]
            {
                instanceIdPrefixGuid,
                instanceIdPrefixGuid + "_0_Foo",
                instanceIdPrefixGuid + "_1_Bar",
                instanceIdPrefixGuid + "_Foo",
                instanceIdPrefixGuid + "_Bar",
            };

            // Create multiple instances that we'll try to query back
            await host.StartAsync();

            TestOrchestrationClient client;
            foreach (string instanceId in instanceIds)
            {
                client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), input: "Greetings!", instanceId);
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));
            }

            // Add one more instance which shouldn't get picked up
            client = await host.StartOrchestrationAsync(
                typeof(Orchestrations.Echo),
                input: "Greetings!",
                instanceId: $"Foo_{instanceIdPrefixGuid}");
            await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

            DurableStatusQueryResult queryResult = await host.service.GetOrchestrationStateAsync(
                new OrchestrationInstanceStatusQueryCondition()
                {
                    InstanceIdPrefix = instanceIdPrefixGuid,
                },
                top: instanceIds.Length,
                continuationToken: null);
            Assert.AreEqual(instanceIds.Length, queryResult.OrchestrationState.Count());
            Assert.IsNull(queryResult.ContinuationToken);

            await host.StopAsync();
        }

        [TestMethod]
        public async Task NoInstancesGetAllOrchestrationStatusesNullContinuationToken()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                // Execute the orchestrator twice. Orchestrator will be replied. However instances might be two.
                await host.StartAsync();
                var queryResult = await host.service.GetOrchestrationStateAsync(
                    new OrchestrationInstanceStatusQueryCondition(),
                    100,
                    null);

                Assert.IsNull(queryResult.ContinuationToken);
                await host.StopAsync();
            }
        }

        [DataTestMethod]
        [DataRow(false, false)]
        [DataRow(true, false)]
        [DataRow(false, true)]
        [DataRow(true, true)]
        public async Task EventConversation(bool enableExtendedSessions, bool useFireAndForget)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(typeof(Test.Orchestrations.EventConversationOrchestration), useFireAndForget);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual("OK", JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task AutoStart(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                host.AddAutoStartOrchestrator(typeof(Orchestrations.AutoStartOrchestration.Responder));

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.AutoStartOrchestration), "");
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual("OK", JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task ContinueAsNewThenTimer(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(typeof(Test.Orchestrations.ContinueAsNewThenTimerOrchestration), 0);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual("OK", JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task CallCounterEntityFromOrchestration(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                var entityId = new EntityId(nameof(Entities.Counter), Guid.NewGuid().ToString());
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.CallCounterEntity), entityId);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual("OK", JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task BatchedEntitySignals(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();
            
                int numIterations = 100;
                var entityId = new EntityId(nameof(Entities.BatchEntity), Guid.NewGuid().ToString());
                TestEntityClient client = await host.GetEntityClientAsync(typeof(Entities.BatchEntity), entityId);

                // send a number of signals immediately after each other
                List<Task> tasks = new List<Task>();
                for (int i = 0; i < numIterations; i++)
                {
                    tasks.Add(client.SignalEntity(i.ToString()));
                }

                await Task.WhenAll(tasks);

                var result = await client.WaitForEntityState<List<(int, int)>>( 
                    timeout: Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(20),
                    list => list.Count == numIterations ? null : $"waiting for {numIterations - list.Count} signals");

                // validate the batching positions and sizes
                int? cursize = null;
                int curpos = 0;
                int numBatches = 0;
                foreach (var (position, size) in result)
                {
                    if (cursize == null)
                    {
                        cursize = size;
                        curpos = 0;
                        numBatches++;
                    }

                    Assert.AreEqual(curpos, position);

                    if (++curpos == cursize)
                    {
                        cursize = null;
                    }
                }

                // there should always be some batching going on
                Assert.IsTrue(numBatches < numIterations);

                await host.StopAsync();
            }
        }

        [DataTestMethod]
        public async Task CleanEntityStorage_OrphanedLock()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(
                enableExtendedSessions: false,
                modifySettingsAction: (settings) => settings.EntityMessageReorderWindowInMinutes = 0))
            {
                await host.StartAsync();

                // construct unique names for this test
                string prefix = Guid.NewGuid().ToString("N").Substring(0, 6);
                var orphanedEntityId = new EntityId(nameof(Entities.Counter), $"{prefix}-orphaned");
                var orchestrationA = $"{prefix}-A";
                var orchestrationB = $"{prefix}-B";

                // run an orchestration A that leaves an orphaned lock
                var clientA = await host.StartOrchestrationAsync(typeof(Orchestrations.LockThenFailReplay), (orphanedEntityId, true), orchestrationA);
                var status = await clientA.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                // run an orchestration B that queues behind A for the lock (and thus gets stuck)
                var clientB = await host.StartOrchestrationAsync(typeof(Orchestrations.LockThenFailReplay), (orphanedEntityId, false), orchestrationB);

                // remove empty entity and release orphaned lock
                var entityClient = new TaskHubEntityClient(clientA.InnerClient);
                var response = await entityClient.CleanEntityStorageAsync(true, true, CancellationToken.None);
                Assert.AreEqual(1, response.NumberOfOrphanedLocksRemoved);
                Assert.AreEqual(0, response.NumberOfEmptyEntitiesRemoved);

                // wait for orchestration B to complete, now that the lock has been released
                status = await clientB.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                Assert.IsTrue(status.OrchestrationStatus == OrchestrationStatus.Completed);

                // give the orchestration status time to be updated
                await Task.Delay(TimeSpan.FromSeconds(20));

                // clean again to remove the orphaned entity which is now empty also
                response = await entityClient.CleanEntityStorageAsync(true, true, CancellationToken.None);
                Assert.AreEqual(0, response.NumberOfOrphanedLocksRemoved);
                Assert.AreEqual(1, response.NumberOfEmptyEntitiesRemoved);

                await host.StopAsync();
            }
        }

        [DataTestMethod]
        [DataRow(1)]
        [DataRow(120)]
        public async Task CleanEntityStorage_EmptyEntities(int numReps)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(
                enableExtendedSessions: false,
                modifySettingsAction: (settings) => settings.EntityMessageReorderWindowInMinutes = 0))
            {
                await host.StartAsync();

                // construct unique names for this test
                string prefix = Guid.NewGuid().ToString("N").Substring(0, 6);
                EntityId[] entityIds = new EntityId[numReps];
                for (int i = 0; i < entityIds.Length; i++)
                {
                    entityIds[i] = new EntityId("Counter", $"{prefix}-{i:D3}");
                }

                // create the empty entities
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.CreateEmptyEntities), entityIds);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                // account for delay in updating instance tables
                await Task.Delay(TimeSpan.FromSeconds(20));

                // remove all empty entities
                var entityClient = new TaskHubEntityClient(client.InnerClient);
                var response = await entityClient.CleanEntityStorageAsync(true, true, CancellationToken.None);
                Assert.AreEqual(0, response.NumberOfOrphanedLocksRemoved);
                Assert.AreEqual(numReps, response.NumberOfEmptyEntitiesRemoved);

                await host.StopAsync();
            }
        }

        [DataTestMethod]
        public async Task EntityQueries()
        {
            var yesterday = DateTime.UtcNow.Subtract(TimeSpan.FromDays(1));
            var tomorrow = DateTime.UtcNow.Add(TimeSpan.FromDays(1));

            List<EntityId> entityIds = new List<EntityId>()
            {
                new EntityId("StringStore", "foo"),
                new EntityId("StringStore", "bar"),
                new EntityId("StringStore", "baz"),
                new EntityId("StringStore2", "foo"),
            };

            var queries = new (TaskHubEntityClient.Query,Action<IList<TaskHubEntityClient.EntityStatus>>)[]
            {
                (new TaskHubEntityClient.Query
                {
                },
                result =>
                {
                    Assert.AreEqual(4, result.Count);
                }),

                (new TaskHubEntityClient.Query
                {
                    EntityName = "StringStore",
                    LastOperationFrom = yesterday,
                    LastOperationTo = tomorrow,
                    FetchState = false,
                }, 
                result =>
                {
                    Assert.AreEqual(3, result.Count);
                    Assert.IsTrue(result[0].State == null);
                }),

                (new TaskHubEntityClient.Query
                {
                    EntityName = "StringStore",
                    LastOperationFrom = yesterday,
                    LastOperationTo = tomorrow,
                    FetchState = true,
                },
                result =>
                {
                    Assert.AreEqual(3, result.Count);
                    Assert.IsTrue(result[0].State != null);
                }),

                (new TaskHubEntityClient.Query
                {
                    EntityName = "StringStore",
                    PageSize = 1,
                },
                result =>
                {
                   Assert.AreEqual(3, result.Count());
                }),

                (new TaskHubEntityClient.Query
                {
                    EntityName = "StringStore",
                    PageSize = 2,
                },
                result =>
                {
                     Assert.AreEqual(3, result.Count());
                }),

                (new TaskHubEntityClient.Query
                {
                    EntityName = "noResult",
                },
                result =>
                {
                    Assert.AreEqual(0, result.Count());
                }),

                (new TaskHubEntityClient.Query
                {
                    EntityName = "noResult",
                    LastOperationFrom = yesterday,
                    LastOperationTo = tomorrow,
                },
                result =>
                {
                    Assert.AreEqual(0, result.Count());
                }),

                (new TaskHubEntityClient.Query
                {
                    LastOperationFrom = tomorrow,
                },
                result =>
                {
                    Assert.AreEqual(0, result.Count());
                }),

                (new TaskHubEntityClient.Query
                {
                    LastOperationTo = yesterday,
                },
                result =>
                {
                    Assert.AreEqual(0, result.Count());
                }),

                (new TaskHubEntityClient.Query
                {
                    InstanceIdPrefix = "StringStore",
                },
                result =>
                {
                    Assert.AreEqual(0, result.Count());
                }),

                 (new TaskHubEntityClient.Query
                {
                    InstanceIdPrefix = "@StringStore",
                },
                result =>
                {
                    Assert.AreEqual(0, result.Count());
                }),

                (new TaskHubEntityClient.Query
                {
                    InstanceIdPrefix = "@stringstore",
                },
                result =>
                {
                    Assert.AreEqual(4, result.Count());
                }),

                (new TaskHubEntityClient.Query
                {
                    InstanceIdPrefix = "@stringstore@",
                },
                result =>
                {
                    Assert.AreEqual(3, result.Count());
                }),

                (new TaskHubEntityClient.Query
                {
                    InstanceIdPrefix = "@stringstore",
                    EntityName = "stringstore",
                },
                result =>
                {
                    Assert.AreEqual(3, result.Count());
                }),

                (new TaskHubEntityClient.Query
                {
                    InstanceIdPrefix = "@stringstore@",
                    EntityName = "StringStore",
                },
                result =>
                {
                    Assert.AreEqual(3, result.Count());
                }),

                (new TaskHubEntityClient.Query
                {
                    InstanceIdPrefix = "@stringstore@b",
                    EntityName = "StringStore",
                },
                result =>
                {
                    Assert.AreEqual(2, result.Count());
                }),
            };

             await this.RunEntityQueries(queries, entityIds); 
        }

        [DataTestMethod]
        public async Task EntityQueries_Deleted()
        {
            var queries = new (TaskHubEntityClient.Query, Action<IList<TaskHubEntityClient.EntityStatus>>)[]
            {
                (new TaskHubEntityClient.Query()
                {
                    IncludeDeleted = false,
                },
                result =>
                {
                    Assert.AreEqual(4, result.Count);
                }),

                (new TaskHubEntityClient.Query()
                {
                    IncludeDeleted = true,
                },
                result =>
                {
                    Assert.AreEqual(8, result.Count);
                }),

                (new TaskHubEntityClient.Query()
                {
                    IncludeDeleted = false,
                    PageSize = 3,
                },
                result =>
                {
                    Assert.AreEqual(4, result.Count);
                }),

                (new TaskHubEntityClient.Query()
                {
                    IncludeDeleted = true,
                    PageSize = 3,
                },
                result =>
                {
                    Assert.AreEqual(8, result.Count);
                }),
            };

            List<EntityId> entityIds = new List<EntityId>()
            {
                new EntityId("StringStore", "foo"),
                new EntityId("StringStore2", "bar"),
                new EntityId("StringStore2", "baz"),
                new EntityId("StringStore2", "foo"),
                new EntityId("StringStore2", "ffo"),
                new EntityId("StringStore2", "zzz"),
                new EntityId("StringStore2", "aaa"),
                new EntityId("StringStore2", "bbb"),
            };

            List<Type> orchestrations = new List<Type>()
            {
                typeof(Orchestrations.SignalAndCallStringStore),
                typeof(Orchestrations.CallAndDeleteStringStore),
                typeof(Orchestrations.SignalAndCallStringStore),
                typeof(Orchestrations.CallAndDeleteStringStore),
                typeof(Orchestrations.SignalAndCallStringStore),
                typeof(Orchestrations.CallAndDeleteStringStore),
                typeof(Orchestrations.SignalAndCallStringStore),
                typeof(Orchestrations.CallAndDeleteStringStore),
            };

            await this.RunEntityQueries(queries, entityIds, orchestrations);       
        }

        private async Task RunEntityQueries(
            (TaskHubEntityClient.Query, Action<IList<TaskHubEntityClient.EntityStatus>>)[] queries, 
            IList<EntityId> entitiyIds, 
            IList<Type> orchestrations = null)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                await host.StartAsync();

                TestOrchestrationClient client = null;

                for (int i = 0; i < entitiyIds.Count; i++)
                {
                    EntityId id = entitiyIds[i];
                    Type orchestration = orchestrations == null ? typeof(Orchestrations.SignalAndCallStringStore) : orchestrations[i];
                    client = await host.StartOrchestrationAsync(orchestration, id);
                    await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                }

                // account for delay in updating instance tables
                await Task.Delay(TimeSpan.FromSeconds(10));

                for (int i = 0; i < queries.Count(); i++)
                {
                    var query = queries[i].Item1;
                    var test = queries[i].Item2;
                    var results = new List<TaskHubEntityClient.EntityStatus>();
                    var entityClient = new TaskHubEntityClient(client.InnerClient);

                    do
                    {
                        var result = await entityClient.ListEntitiesAsync(query, CancellationToken.None);

                        // The result may return fewer records than the page size, but never more
                        Assert.IsTrue(result.Entities.Count() <= query.PageSize);

                        foreach (var element in result.Entities)
                        {
                            results.Add(element);
                        }

                        query.ContinuationToken = result.ContinuationToken;
                    }
                    while (query.ContinuationToken != null);

                    test(results);
                }

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates launching orchestrations from entities.
        /// </summary>
        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task EntityFireAndForget(bool extendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: extendedSessions))
            {
                await host.StartAsync();

                var entityId = new EntityId(nameof(Entities.Launcher), Guid.NewGuid().ToString());

                var client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.LaunchOrchestrationFromEntity),
                    entityId);

                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(90));
                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

                var instanceId = (string) JToken.Parse(status?.Output);
                Assert.IsTrue(instanceId != null);
                var orchestrationState = (await client.InnerClient.GetOrchestrationStateAsync(instanceId, false)).FirstOrDefault();       
                Assert.AreEqual(OrchestrationStatus.Completed, orchestrationState.OrchestrationStatus);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates a simple entity scenario which sends a signal
        /// to a relay which forwards it to counter, and polls until the signal is delivered.
        /// </summary>
        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task DurableEntity_SignalThenPoll(bool extendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: extendedSessions))
            {
                await host.StartAsync();

                var relayEntityId = new EntityId("Relay", "");
                var counterEntityId = new EntityId("Counter", Guid.NewGuid().ToString());

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.PollCounterEntity), counterEntityId);

                var entityClient = new TaskHubEntityClient(client.InnerClient);
                await entityClient.SignalEntityAsync(relayEntityId, "", (counterEntityId, "increment"));

                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual("ok", (string)JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates an entity scenario where a "LockedTransfer" orchestration locks
        /// two "Counter" entities, and then in parallel increments/decrements them, respectively, using
        /// a read-modify-write pattern.
        /// </summary>
        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task DurableEntity_SingleLockedTransfer(bool extendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: extendedSessions))
            {
                await host.StartAsync();

                var counter1 = new EntityId("Counter", Guid.NewGuid().ToString());
                var counter2 = new EntityId("Counter", Guid.NewGuid().ToString());

                var client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.LockedTransfer),
                    (counter1, counter2));

                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

                // validate the state of the counters
                var entityClient = new TaskHubEntityClient(client.InnerClient);
                var response1 = await entityClient.ReadEntityStateAsync<int>(counter1);
                var response2 = await entityClient.ReadEntityStateAsync<int>(counter2);
                Assert.IsTrue(response1.EntityExists);
                Assert.IsTrue(response2.EntityExists);
                Assert.AreEqual(-1, response1.EntityState);
                Assert.AreEqual(1, response2.EntityState);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates an entity scenario where a a number of "LockedTransfer" orchestrations
        /// concurrently operate on a number of entities, in a classical dining-philosophers configuration.
        /// This showcases the deadlock prevention mechanism achieved by the sequential, ordered lock acquisition.
        /// </summary>
        [DataTestMethod]
        [DataRow(false, 5)]
        [DataRow(true, 5)]
        public async Task DurableEntity_MultipleLockedTransfers(bool extendedSessions, int numberEntities)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: extendedSessions))
            {
                await host.StartAsync();

                // create specified number of entities
                var counters = new EntityId[numberEntities];
                for (int i = 0; i < numberEntities; i++)
                {
                    counters[i] = new EntityId("Counter", Guid.NewGuid().ToString());
                }

                // in parallel, start one transfer per counter, each decrementing a counter and incrementing
                // its successor (where the last one wraps around to the first)
                // This is a pattern that would deadlock if we didn't order the lock acquisition.
                var clients = new Task<TestOrchestrationClient>[numberEntities];
                for (int i = 0; i < numberEntities; i++)
                {
                    clients[i] = host.StartOrchestrationAsync(
                        typeof(Orchestrations.LockedTransfer),
                        (counters[i], counters[(i + 1) % numberEntities]));
                }

                await Task.WhenAll(clients);

                // in parallel, wait for all transfers to complete
                var stati = new Task<OrchestrationState>[numberEntities];
                for (int i = 0; i < numberEntities; i++)
                {
                    stati[i] = clients[i].Result.WaitForCompletionAsync(TimeSpan.FromSeconds(60));
                }

                await Task.WhenAll(stati);

                // check that they all completed
                for (int i = 0; i < numberEntities; i++)
                {
                    Assert.AreEqual(OrchestrationStatus.Completed, stati[i].Result.OrchestrationStatus);
                }

                // in parallel, read all the entity states
                var entityClient = new TaskHubEntityClient(clients[0].Result.InnerClient);
                var entityStates = new Task<TaskHubEntityClient.StateResponse<int>>[numberEntities];
                for (int i = 0; i < numberEntities; i++)
                {
                    entityStates[i] = entityClient.ReadEntityStateAsync<int>(counters[i]);
                }
                await Task.WhenAll(entityStates);

                // check that the counter states are all back to 0
                // (since each participated in 2 transfers, one incrementing and one decrementing)
                for (int i = 0; i < numberEntities; i++)
                {
                    Assert.IsTrue(entityStates[i].Result.EntityExists);
                    Assert.AreEqual(0, entityStates[i].Result.EntityState);
                }

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates exception handling in entity operations.
        /// </summary>
        [DataTestMethod]
        [DataRow(false, false, false)]
        [DataRow(false, true, false)]
        [DataRow(true, false, false)]
        [DataRow(true, true, false)]
        [DataRow(false, false, true)]
        public async Task CallFaultyEntity(bool extendedSessions, bool rollbackOnExceptions, bool useFailureDetails)
        {
            var errorPropagationMode = useFailureDetails ? ErrorPropagationMode.UseFailureDetails : ErrorPropagationMode.SerializeExceptions;

            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(
                enableExtendedSessions: extendedSessions,
                errorPropagationMode: errorPropagationMode))
            {
                await host.StartAsync();

                var entityName = rollbackOnExceptions ? "FaultyEntityWithRollback" : "FaultyEntityWithoutRollback";
                var entityId = new EntityId(entityName, Guid.NewGuid().ToString());

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.CallFaultyEntity), (entityId, rollbackOnExceptions, errorPropagationMode));
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(90));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual("\"ok\"", status?.Output);

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task PurgeInstanceHistoryForSingleInstanceWithoutLargeMessageBlobs()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                string instanceId = Guid.NewGuid().ToString();
                await host.StartAsync();
                TestOrchestrationClient client = await host.StartOrchestrationAsync(typeof(Orchestrations.Factorial), 110, instanceId);
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                List<HistoryStateEvent> historyEvents = await client.GetOrchestrationHistoryAsync(instanceId);
                Assert.IsTrue(historyEvents.Count > 0);

                IList<OrchestrationState> orchestrationStateList = await client.GetStateAsync(instanceId);
                Assert.AreEqual(1, orchestrationStateList.Count);
                Assert.AreEqual(instanceId, orchestrationStateList.First().OrchestrationInstance.InstanceId);

                await client.PurgeInstanceHistory();

                List<HistoryStateEvent> historyEventsAfterPurging = await client.GetOrchestrationHistoryAsync(instanceId);
                Assert.AreEqual(0, historyEventsAfterPurging.Count);

                orchestrationStateList = await client.GetStateAsync(instanceId);
                Assert.AreEqual(1, orchestrationStateList.Count);
                Assert.IsNull(orchestrationStateList.First());

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task ValidateCustomStatusPersists()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(false))
            {
                await host.StartAsync();

                string customStatus = "custom_status";
                var client = await host.StartOrchestrationAsync(
                    typeof(Test.Orchestrations.ChangeStatusOrchestration),
                    new string[] { customStatus });
                var state = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, state?.OrchestrationStatus);
                Assert.AreEqual(customStatus, JToken.Parse(state?.Status));

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task ValidateNullCustomStatusPersists()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(false))
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(
                    typeof(Test.Orchestrations.ChangeStatusOrchestration),
                    // First set "custom_status", then set null and make sure it persists
                    new string[] { "custom_status", null });
                var state = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, state?.OrchestrationStatus);
                Assert.AreEqual(null, JToken.Parse(state?.Status).Value<string>());

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task PurgeInstanceHistoryForSingleInstanceWithLargeMessageBlobs()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                await host.StartAsync();

                string instanceId = Guid.NewGuid().ToString();
                string message = this.GenerateMediumRandomStringPayload().ToString();
                TestOrchestrationClient client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message, instanceId);
                OrchestrationState status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));
                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

                List<HistoryStateEvent> historyEvents = await client.GetOrchestrationHistoryAsync(instanceId);
                Assert.IsTrue(historyEvents.Count > 0);

                IList<OrchestrationState> orchestrationStateList = await client.GetStateAsync(instanceId);
                Assert.AreEqual(1, orchestrationStateList.Count);
                Assert.AreEqual(instanceId, orchestrationStateList.First().OrchestrationInstance.InstanceId);

                int blobCount = await this.GetBlobCount("test-largemessages", instanceId);
                Assert.IsTrue(blobCount > 0);

                IList<OrchestrationState> results = await host.GetAllOrchestrationInstancesAsync();
                Assert.AreEqual(1, results.Count);

                string result = JToken.Parse(results.First(x => x.OrchestrationInstance.InstanceId == instanceId).Output).ToString();
                Assert.AreEqual(message, result);

                await client.PurgeInstanceHistory();


                List<HistoryStateEvent> historyEventsAfterPurging = await client.GetOrchestrationHistoryAsync(instanceId);
                Assert.AreEqual(0, historyEventsAfterPurging.Count);

                orchestrationStateList = await client.GetStateAsync(instanceId);
                Assert.AreEqual(1, orchestrationStateList.Count);
                Assert.IsNull(orchestrationStateList.First());

                blobCount = await this.GetBlobCount("test-largemessages", instanceId);
                Assert.AreEqual(0, blobCount);

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task PurgeInstanceHistoryForTimePeriodDeleteAll()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                await host.StartAsync();
                DateTime startDateTime = DateTime.Now;
                string firstInstanceId = "instance1";
                TestOrchestrationClient client = await host.StartOrchestrationAsync(typeof(Orchestrations.FanOutFanIn), 50, firstInstanceId);
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                string secondInstanceId = "instance2";
                client = await host.StartOrchestrationAsync(typeof(Orchestrations.FanOutFanIn), 50, secondInstanceId);
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                string thirdInstanceId = "instance3";
                client = await host.StartOrchestrationAsync(typeof(Orchestrations.FanOutFanIn), 50, thirdInstanceId);
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                string fourthInstanceId = "instance4";
                string message = this.GenerateMediumRandomStringPayload().ToString();
                client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message, fourthInstanceId);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));
                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

                IList<OrchestrationState> results = await host.GetAllOrchestrationInstancesAsync();
                Assert.AreEqual(4, results.Count);
                Assert.AreEqual("\"Done\"", results.First(x => x.OrchestrationInstance.InstanceId == firstInstanceId).Output);
                Assert.AreEqual("\"Done\"", results.First(x => x.OrchestrationInstance.InstanceId == secondInstanceId).Output);
                Assert.AreEqual("\"Done\"", results.First(x => x.OrchestrationInstance.InstanceId == thirdInstanceId).Output);
                string result = JToken.Parse(results.First(x => x.OrchestrationInstance.InstanceId == fourthInstanceId).Output).ToString();
                Assert.AreEqual(message, result);

                List<HistoryStateEvent> firstHistoryEvents = await client.GetOrchestrationHistoryAsync(firstInstanceId);
                Assert.IsTrue(firstHistoryEvents.Count > 0);

                List<HistoryStateEvent> secondHistoryEvents = await client.GetOrchestrationHistoryAsync(secondInstanceId);
                Assert.IsTrue(secondHistoryEvents.Count > 0);

                List<HistoryStateEvent> thirdHistoryEvents = await client.GetOrchestrationHistoryAsync(thirdInstanceId);
                Assert.IsTrue(thirdHistoryEvents.Count > 0);

                List<HistoryStateEvent> fourthHistoryEvents = await client.GetOrchestrationHistoryAsync(thirdInstanceId);
                Assert.IsTrue(fourthHistoryEvents.Count > 0);

                IList<OrchestrationState> firstOrchestrationStateList = await client.GetStateAsync(firstInstanceId);
                Assert.AreEqual(1, firstOrchestrationStateList.Count);
                Assert.AreEqual(firstInstanceId, firstOrchestrationStateList.First().OrchestrationInstance.InstanceId);

                IList<OrchestrationState> secondOrchestrationStateList = await client.GetStateAsync(secondInstanceId);
                Assert.AreEqual(1, secondOrchestrationStateList.Count);
                Assert.AreEqual(secondInstanceId, secondOrchestrationStateList.First().OrchestrationInstance.InstanceId);

                IList<OrchestrationState> thirdOrchestrationStateList = await client.GetStateAsync(thirdInstanceId);
                Assert.AreEqual(1, thirdOrchestrationStateList.Count);
                Assert.AreEqual(thirdInstanceId, thirdOrchestrationStateList.First().OrchestrationInstance.InstanceId);

                IList<OrchestrationState> fourthOrchestrationStateList = await client.GetStateAsync(fourthInstanceId);
                Assert.AreEqual(1, fourthOrchestrationStateList.Count);
                Assert.AreEqual(fourthInstanceId, fourthOrchestrationStateList.First().OrchestrationInstance.InstanceId);

                int blobCount = await this.GetBlobCount("test-largemessages", fourthInstanceId);
                Assert.AreEqual(6, blobCount);

                await client.PurgeInstanceHistoryByTimePeriod(
                    startDateTime,
                    DateTime.UtcNow,
                    new List<OrchestrationStatus>
                    {
                        OrchestrationStatus.Completed,
                        OrchestrationStatus.Terminated,
                        OrchestrationStatus.Failed,
                        OrchestrationStatus.Running
                    });

                List<HistoryStateEvent> firstHistoryEventsAfterPurging = await client.GetOrchestrationHistoryAsync(firstInstanceId);
                Assert.AreEqual(0, firstHistoryEventsAfterPurging.Count);

                List<HistoryStateEvent> secondHistoryEventsAfterPurging = await client.GetOrchestrationHistoryAsync(secondInstanceId);
                Assert.AreEqual(0, secondHistoryEventsAfterPurging.Count);

                List<HistoryStateEvent> thirdHistoryEventsAfterPurging = await client.GetOrchestrationHistoryAsync(thirdInstanceId);
                Assert.AreEqual(0, thirdHistoryEventsAfterPurging.Count);

                List<HistoryStateEvent>fourthHistoryEventsAfterPurging = await client.GetOrchestrationHistoryAsync(fourthInstanceId);
                Assert.AreEqual(0, fourthHistoryEventsAfterPurging.Count);

                firstOrchestrationStateList = await client.GetStateAsync(firstInstanceId);
                Assert.AreEqual(1, firstOrchestrationStateList.Count);
                Assert.IsNull(firstOrchestrationStateList.First());

                secondOrchestrationStateList = await client.GetStateAsync(secondInstanceId);
                Assert.AreEqual(1, secondOrchestrationStateList.Count);
                Assert.IsNull(secondOrchestrationStateList.First());

                thirdOrchestrationStateList = await client.GetStateAsync(thirdInstanceId);
                Assert.AreEqual(1, thirdOrchestrationStateList.Count);
                Assert.IsNull(thirdOrchestrationStateList.First());

                fourthOrchestrationStateList = await client.GetStateAsync(fourthInstanceId);
                Assert.AreEqual(1, fourthOrchestrationStateList.Count);
                Assert.IsNull(fourthOrchestrationStateList.First());

                blobCount = await this.GetBlobCount("test-largemessages", fourthInstanceId);
                Assert.AreEqual(0, blobCount);

                await host.StopAsync();
            }
        }

        private async Task<int> GetBlobCount(string containerName, string directoryName)
        {
            string storageConnectionString = TestHelpers.GetTestStorageAccountConnectionString();
            CloudStorageAccount storageAccount;
            if (!CloudStorageAccount.TryParse(storageConnectionString, out storageAccount))
            {
                Assert.Fail("Couldn't find the connection string to use to look up blobs!");
                return 0;
            }

            CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();

            CloudBlobContainer cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);
            await cloudBlobContainer.CreateIfNotExistsAsync();
            CloudBlobDirectory instanceDirectory = cloudBlobContainer.GetDirectoryReference(directoryName);
            var blobs = new List<IListBlobItem>();
            BlobContinuationToken blobContinuationToken = null;
            do
            {
                BlobResultSegment results = await TimeoutHandler.ExecuteWithTimeout("GetBlobCount", "dummyAccount", new AzureStorageOrchestrationServiceSettings(), (context, timeoutToken) =>
                {
                    return instanceDirectory.ListBlobsSegmentedAsync(
                            useFlatBlobListing: true,
                            blobListingDetails: BlobListingDetails.Metadata,
                            maxResults: null,
                            currentToken: blobContinuationToken,
                            options: null,
                            operationContext: context,
                            cancellationToken: timeoutToken);
                });
                
                blobContinuationToken = results.ContinuationToken;
                blobs.AddRange(results.Results);
            } while (blobContinuationToken != null);

            Trace.TraceInformation(
                "Found {0} blobs: {1}{2}",
                blobs.Count,
                Environment.NewLine,
                string.Join(Environment.NewLine, blobs.Select(b => b.Uri)));
            return blobs.Count;
        }


        [TestMethod]
        public async Task PurgeInstanceHistoryForTimePeriodDeletePartially()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                // Execute the orchestrator twice. Orchestrator will be replied. However instances might be two.
                await host.StartAsync();
                DateTime startDateTime = DateTime.Now;
                string firstInstanceId = Guid.NewGuid().ToString();
                TestOrchestrationClient client = await host.StartOrchestrationAsync(typeof(Orchestrations.FanOutFanIn), 50, firstInstanceId);
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                DateTime endDateTime = DateTime.Now;
                await Task.Delay(5000);
                string secondInstanceId = Guid.NewGuid().ToString();
                client = await host.StartOrchestrationAsync(typeof(Orchestrations.FanOutFanIn), 50, secondInstanceId);
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                string thirdInstanceId = Guid.NewGuid().ToString();
                client = await host.StartOrchestrationAsync(typeof(Orchestrations.FanOutFanIn), 50, thirdInstanceId);
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                IList<OrchestrationState> results = await host.GetAllOrchestrationInstancesAsync();
                Assert.AreEqual(3, results.Count);
                Assert.IsNotNull(results[0].Output.Equals("\"Done\""));
                Assert.IsNotNull(results[1].Output.Equals("\"Done\""));
                Assert.IsNotNull(results[2].Output.Equals("\"Done\""));


                List<HistoryStateEvent> firstHistoryEvents = await client.GetOrchestrationHistoryAsync(firstInstanceId);
                Assert.IsTrue(firstHistoryEvents.Count > 0);

                List<HistoryStateEvent> secondHistoryEvents = await client.GetOrchestrationHistoryAsync(secondInstanceId);
                Assert.IsTrue(secondHistoryEvents.Count > 0);

                List<HistoryStateEvent> thirdHistoryEvents = await client.GetOrchestrationHistoryAsync(thirdInstanceId);
                Assert.IsTrue(secondHistoryEvents.Count > 0);

                IList<OrchestrationState> firstOrchestrationStateList = await client.GetStateAsync(firstInstanceId);
                Assert.AreEqual(1, firstOrchestrationStateList.Count);
                Assert.AreEqual(firstInstanceId, firstOrchestrationStateList.First().OrchestrationInstance.InstanceId);

                IList<OrchestrationState> secondOrchestrationStateList = await client.GetStateAsync(secondInstanceId);
                Assert.AreEqual(1, secondOrchestrationStateList.Count);
                Assert.AreEqual(secondInstanceId, secondOrchestrationStateList.First().OrchestrationInstance.InstanceId);

                IList<OrchestrationState> thirdOrchestrationStateList = await client.GetStateAsync(thirdInstanceId);
                Assert.AreEqual(1, thirdOrchestrationStateList.Count);
                Assert.AreEqual(thirdInstanceId, thirdOrchestrationStateList.First().OrchestrationInstance.InstanceId);

                await client.PurgeInstanceHistoryByTimePeriod(startDateTime, endDateTime, new List<OrchestrationStatus> { OrchestrationStatus.Completed, OrchestrationStatus.Terminated, OrchestrationStatus.Failed, OrchestrationStatus.Running });

                List<HistoryStateEvent> firstHistoryEventsAfterPurging = await client.GetOrchestrationHistoryAsync(firstInstanceId);
                Assert.AreEqual(0, firstHistoryEventsAfterPurging.Count);

                List<HistoryStateEvent> secondHistoryEventsAfterPurging = await client.GetOrchestrationHistoryAsync(secondInstanceId);
                Assert.IsTrue(secondHistoryEventsAfterPurging.Count > 0);

                List<HistoryStateEvent> thirdHistoryEventsAfterPurging = await client.GetOrchestrationHistoryAsync(thirdInstanceId);
                Assert.IsTrue(thirdHistoryEventsAfterPurging.Count > 0);

                firstOrchestrationStateList = await client.GetStateAsync(firstInstanceId);
                Assert.AreEqual(1, firstOrchestrationStateList.Count);
                Assert.IsNull(firstOrchestrationStateList.First());

                secondOrchestrationStateList = await client.GetStateAsync(secondInstanceId);
                Assert.AreEqual(1, secondOrchestrationStateList.Count);
                Assert.AreEqual(secondInstanceId, secondOrchestrationStateList.First().OrchestrationInstance.InstanceId);

                thirdOrchestrationStateList = await client.GetStateAsync(thirdInstanceId);
                Assert.AreEqual(1, thirdOrchestrationStateList.Count);
                Assert.AreEqual(thirdInstanceId, thirdOrchestrationStateList.First().OrchestrationInstance.InstanceId);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates parallel function execution by enumerating all files in the current directory 
        /// in parallel and getting the sum total of all file sizes.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task ParallelOrchestration(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.DiskUsage), Environment.CurrentDirectory);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(90));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(Environment.CurrentDirectory, JToken.Parse(status?.Input));
                Assert.IsTrue(long.Parse(status?.Output) > 0L);

                await host.StopAsync();
            }
        }

        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task LargeFanOutOrchestration(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.FanOutFanIn), 1000);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task FanOutOrchestration_LargeHistoryBatches()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
            {
                await host.StartAsync();

                // This test creates history payloads that exceed the 4 MB limit imposed by Azure Storage
                // when 100 entities are uploaded at a time.
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.SemiLargePayloadFanOutFanIn), 90);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates the ContinueAsNew functionality by implementing a counter actor pattern.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task ActorOrchestration(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                int initialValue = 0;
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Counter), initialValue);

                // Need to wait for the instance to start before sending events to it.
                // TODO: This requirement may not be ideal and should be revisited.
                await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

                // Perform some operations
                await client.RaiseEventAsync("operation", "incr");
                await client.RaiseEventAsync("operation", "incr");
                await client.RaiseEventAsync("operation", "incr");
                await client.RaiseEventAsync("operation", "decr");
                await client.RaiseEventAsync("operation", "incr");
                await Task.Delay(2000);

                // Make sure it's still running and didn't complete early (or fail).
                var status = await client.GetStatusAsync();
                Assert.IsTrue(
                    status?.OrchestrationStatus == OrchestrationStatus.Running ||
                    status?.OrchestrationStatus == OrchestrationStatus.ContinuedAsNew);

                // The end message will cause the actor to complete itself.
                await client.RaiseEventAsync("operation", "end");

                status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(3, JToken.Parse(status?.Output));

                // When using ContinueAsNew, the original input is discarded and replaced with the most recent state.
                Assert.AreNotEqual(initialValue, JToken.Parse(status?.Input));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates the ContinueAsNew functionality by implementing character counter actor pattern.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task ActorOrchestrationForLargeInput(bool enableExtendedSessions)
        {
            await this.ValidateCharacterCounterIntegrationTest(enableExtendedSessions);
        }

        /// <summary>
        /// End-to-end test which validates the deletion of all data generated by the ContinueAsNew functionality in the character counter actor pattern.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task ActorOrchestrationDeleteAllLargeMessageBlobs(bool enableExtendedSessions)
        {
            DateTime startDateTime = DateTime.UtcNow;

            Tuple<string, TestOrchestrationClient> resultTuple = await this.ValidateCharacterCounterIntegrationTest(enableExtendedSessions);
            string instanceId = resultTuple.Item1;
            TestOrchestrationClient client = resultTuple.Item2;

            List<HistoryStateEvent> historyEvents = await client.GetOrchestrationHistoryAsync(instanceId);
            Assert.IsTrue(historyEvents.Count > 0);

            IList<OrchestrationState> orchestrationStateList = await client.GetStateAsync(instanceId);
            Assert.AreEqual(1, orchestrationStateList.Count);
            Assert.AreEqual(instanceId, orchestrationStateList.First().OrchestrationInstance.InstanceId);

            int blobCount = await this.GetBlobCount("test-largemessages", instanceId);

            Assert.AreEqual(3, blobCount);

            await client.PurgeInstanceHistoryByTimePeriod(
                startDateTime,
                DateTime.UtcNow,
                new List<OrchestrationStatus>
                {
                    OrchestrationStatus.Completed,
                    OrchestrationStatus.Terminated,
                    OrchestrationStatus.Failed,
                    OrchestrationStatus.Running
                });

            historyEvents = await client.GetOrchestrationHistoryAsync(instanceId);
            Assert.AreEqual(0, historyEvents.Count);

            orchestrationStateList = await client.GetStateAsync(instanceId);
            Assert.AreEqual(1, orchestrationStateList.Count);
            Assert.IsNull(orchestrationStateList.First());

            blobCount = await this.GetBlobCount("test-largemessages", instanceId);
            Assert.AreEqual(0, blobCount);
        }

        private async Task<Tuple<string, TestOrchestrationClient>> ValidateCharacterCounterIntegrationTest(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                string initialMessage = this.GenerateMediumRandomStringPayload().ToString();
                string finalMessage = initialMessage;
                int counter = initialMessage.Length;
                var initialValue = new Tuple<string, int>(initialMessage, counter);
                TestOrchestrationClient client =
                    await host.StartOrchestrationAsync(typeof(Orchestrations.CharacterCounter), initialValue);

                // Need to wait for the instance to start before sending events to it.
                // TODO: This requirement may not be ideal and should be revisited.
                OrchestrationState orchestrationState =
                    await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

                // Perform some operations
                await client.RaiseEventAsync("operation", "double");
                finalMessage = finalMessage + new string(finalMessage.Reverse().ToArray());
                counter *= 2;

                // TODO: Sleeping to avoid a race condition where multiple ContinueAsNew messages
                //       are processed by the same instance at the same time, resulting in a corrupt
                //       storage failure in DTFx.
                await Task.Delay(10000);
                await client.RaiseEventAsync("operation", "double");
                finalMessage = finalMessage + new string(finalMessage.Reverse().ToArray());
                counter *= 2;
                await Task.Delay(10000);
                await client.RaiseEventAsync("operation", "double");
                finalMessage = finalMessage + new string(finalMessage.Reverse().ToArray());
                counter *= 2;
                await Task.Delay(10000);

                // Make sure it's still running and didn't complete early (or fail).
                var status = await client.GetStatusAsync();
                Assert.IsTrue(
                    status?.OrchestrationStatus == OrchestrationStatus.Running ||
                    status?.OrchestrationStatus == OrchestrationStatus.ContinuedAsNew);

                // The end message will cause the actor to complete itself.
                await client.RaiseEventAsync("operation", "end");

                status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                var result = JObject.Parse(status?.Output);
                Assert.IsNotNull(result);

                var input = JObject.Parse(status?.Input);
                Assert.AreEqual(finalMessage, input["Item1"].Value<string>());
                Assert.AreEqual(finalMessage.Length, input["Item2"].Value<int>());
                Assert.AreEqual(finalMessage, result["Item1"].Value<string>());
                Assert.AreEqual(counter, result["Item2"].Value<int>());

                await host.StopAsync();

                return new Tuple<string, TestOrchestrationClient>(
                    orchestrationState.OrchestrationInstance.InstanceId,
                    client);
            }
        }



        /// <summary>
        /// End-to-end test which validates the Terminate functionality.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task TerminateOrchestration(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                // Using the counter orchestration because it will wait indefinitely for input.
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Counter), 0);

                // Need to wait for the instance to start before we can terminate it.
                // TODO: This requirement may not be ideal and should be revisited.
                await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

                await client.TerminateAsync("sayōnara");

                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

                Assert.AreEqual(OrchestrationStatus.Terminated, status?.OrchestrationStatus);
                Assert.AreEqual("sayōnara", status?.Output);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates the Suspend-Resume functionality.
        /// </summary>
        [TestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task SuspendResumeOrchestration(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                string originalStatus = "OGstatus";
                string suspendReason = "sleepyOrch";
                string changedStatus = "newStatus";

                await host.StartAsync();
                var client = await host.StartOrchestrationAsync(typeof(Test.Orchestrations.NextExecution), originalStatus);
                await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

                // Test case 1: Suspend changes the status Running->Suspended
                await client.SuspendAsync(suspendReason);
                var status = await client.WaitForStatusChange(TimeSpan.FromSeconds(10), OrchestrationStatus.Suspended);
                Assert.AreEqual(OrchestrationStatus.Suspended, status?.OrchestrationStatus);
                Assert.AreEqual(suspendReason, status?.Output);

                // Test case 2: external event does not go through
                await client.RaiseEventAsync("changeStatusNow", changedStatus);
                status = await client.GetStatusAsync();
                Assert.AreEqual(originalStatus, JToken.Parse(status?.Status));

                // Test case 3: external event now goes through
                await client.ResumeAsync("wakeUp");
                status  = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));
                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(changedStatus, JToken.Parse(status?.Status));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// Test that a suspended orchestration can be terminated.
        /// </summary>
        [TestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task TerminateSuspendedOrchestration(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Counter), 0);
                await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

                await client.SuspendAsync("suspend");
                await client.WaitForStatusChange(TimeSpan.FromSeconds(10), OrchestrationStatus.Suspended);

                await client.TerminateAsync("terminate");

                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

                Assert.AreEqual(OrchestrationStatus.Terminated, status?.OrchestrationStatus);
                Assert.AreEqual("terminate", status?.Output);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates the Rewind functionality on more than one orchestration.
        /// </summary>
        [TestMethod]
        public async Task RewindOrchestrationsFail()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
            {
                Orchestrations.FactorialOrchestratorFail.ShouldFail = true;
                await host.StartAsync();

                string singletonInstanceId1 = $"1_Test_{Guid.NewGuid():N}";
                string singletonInstanceId2 = $"2_Test_{Guid.NewGuid():N}";

                var client1 = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.FactorialOrchestratorFail),
                    input: 3,
                    instanceId: singletonInstanceId1);

                var statusFail = await client1.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

                Orchestrations.FactorialOrchestratorFail.ShouldFail = false;

                var client2 = await host.StartOrchestrationAsync(
                typeof(Orchestrations.SayHelloWithActivity),
                input: "Catherine",
                instanceId: singletonInstanceId2);

                await client1.RewindAsync("Rewind failed orchestration only");

                var statusRewind = await client1.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, statusRewind?.OrchestrationStatus);
                Assert.AreEqual("6", statusRewind?.Output);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates the Rewind functionality with fan in fan out pattern.
        /// </summary>
        [TestMethod]
        public async Task RewindActivityFailFanOut()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
            {
                Activities.HelloFailFanOut.ShouldFail1 = false;
                await host.StartAsync();

                string singletonInstanceId = $"Test_{Guid.NewGuid():N}";

                var client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.FanOutFanInRewind),
                    input: 3,
                    instanceId: singletonInstanceId);

                var statusFail = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

                Activities.HelloFailFanOut.ShouldFail2 = false;

                await client.RewindAsync("Rewind orchestrator with failed parallel activity.");

                var statusRewind = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, statusRewind?.OrchestrationStatus);
                Assert.AreEqual("\"Done\"", statusRewind?.Output);

                await host.StopAsync();
            }
        }


        /// <summary>
        /// End-to-end test which validates the Rewind functionality on an activity function failure 
        /// with modified (to fail initially) SayHelloWithActivity orchestrator.
        /// </summary>
        [TestMethod]
        public async Task RewindActivityFail()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
            {
                await host.StartAsync();

                string singletonInstanceId = $"{Guid.NewGuid():N}";

                var client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.SayHelloWithActivityFail),
                    input: "World",
                    instanceId: singletonInstanceId);

                var statusFail = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

                Activities.HelloFailActivity.ShouldFail = false;

                await client.RewindAsync("Activity failure test.");

                var statusRewind = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, statusRewind?.OrchestrationStatus);
                Assert.AreEqual("\"Hello, World!\"", statusRewind?.Output);

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task RewindMultipleActivityFail()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
            {
                await host.StartAsync();

                string singletonInstanceId = $"Test_{Guid.NewGuid():N}";

                var client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.FactorialMultipleActivityFail),
                    input: 4,
                    instanceId: singletonInstanceId);

                var statusFail = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

                Activities.MultiplyMultipleActivityFail.ShouldFail1 = false;

                await client.RewindAsync("Rewind for activity failure 1.");

                statusFail = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

                Activities.MultiplyMultipleActivityFail.ShouldFail2 = false;

                await client.RewindAsync("Rewind for activity failure 2.");

                var statusRewind = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, statusRewind?.OrchestrationStatus);
                Assert.AreEqual("24", statusRewind?.Output);

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task RewindSubOrchestrationsTest()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
            {
                await host.StartAsync();

                string ParentInstanceId = $"Parent_{Guid.NewGuid():N}";
                string ChildInstanceId = $"Child_{Guid.NewGuid():N}";

                var clientParent = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.ParentWorkflowSubOrchestrationFail),
                    input: true,
                    instanceId: ParentInstanceId);

                var statusFail = await clientParent.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

                Orchestrations.ChildWorkflowSubOrchestrationFail.ShouldFail1 = false;

                await clientParent.RewindAsync("Rewind first suborchestration failure.");

                statusFail = await clientParent.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

                Orchestrations.ChildWorkflowSubOrchestrationFail.ShouldFail2 = false;

                await clientParent.RewindAsync("Rewind second suborchestration failure.");

                var statusRewind = await clientParent.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, statusRewind?.OrchestrationStatus);

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task RewindSubOrchestrationActivityTest()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
            {
                await host.StartAsync();

                string ParentInstanceId = $"Parent_{Guid.NewGuid():N}";
                string ChildInstanceId = $"Child_{Guid.NewGuid():N}";

                var clientParent = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.ParentWorkflowSubOrchestrationActivityFail),
                    input: true,
                    instanceId: ParentInstanceId);

                var statusFail = await clientParent.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

                Activities.HelloFailSubOrchestrationActivity.ShouldFail1 = false;

                await clientParent.RewindAsync("Rewinding 1: child should still fail.");

                statusFail = await clientParent.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

                Activities.HelloFailSubOrchestrationActivity.ShouldFail2 = false;

                await clientParent.RewindAsync("Rewinding 2: child should complete.");

                var statusRewind = await clientParent.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, statusRewind?.OrchestrationStatus);

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task RewindNestedSubOrchestrationTest()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
            {
                await host.StartAsync();

                string GrandparentInstanceId = $"Grandparent_{Guid.NewGuid():N}";
                string ChildInstanceId = $"Child_{Guid.NewGuid():N}";

                var clientGrandparent = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.GrandparentWorkflowNestedActivityFail),
                    input: true,
                    instanceId: GrandparentInstanceId);

                var statusFail = await clientGrandparent.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

                Activities.HelloFailNestedSuborchestration.ShouldFail1 = false;

                await clientGrandparent.RewindAsync("Rewind 1: Nested child activity still fails.");

                Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

                Activities.HelloFailNestedSuborchestration.ShouldFail2 = false;

                await clientGrandparent.RewindAsync("Rewind 2: Nested child activity completes.");

                var statusRewind = await clientGrandparent.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, statusRewind?.OrchestrationStatus);
                //Assert.AreEqual("\"Hello, Catherine!\"", statusRewind?.Output);

                await host.StopAsync();
            }
        }

        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task TimerCancellation(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                var timeout = TimeSpan.FromSeconds(10);
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Approval), timeout);

                // Need to wait for the instance to start before sending events to it.
                // TODO: This requirement may not be ideal and should be revisited.
                await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));
                await client.RaiseEventAsync("approval", eventData: true);

                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual("Approved", JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates the handling of durable timer expiration.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task TimerExpiration(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                var timeout = TimeSpan.FromSeconds(10);
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Approval), timeout);

                // Need to wait for the instance to start before sending events to it.
                // TODO: This requirement may not be ideal and should be revisited.
                await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

                // Don't send any notification - let the internal timeout expire

                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(20));
                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual("Expired", JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that orchestrations run concurrently of each other (up to 100 by default).
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task OrchestrationConcurrency(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                Func<Task> orchestrationStarter = async delegate ()
                {
                    var timeout = TimeSpan.FromSeconds(10);
                    var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Approval), timeout);
                    await client.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                    // Don't send any notification - let the internal timeout expire
                };

                int iterations = 10;
                var tasks = new Task[iterations];
                for (int i = 0; i < iterations; i++)
                {
                    tasks[i] = orchestrationStarter();
                }

                // The 10 orchestrations above (which each delay for 10 seconds) should all complete in less than 60 seconds.
                Task parallelOrchestrations = Task.WhenAll(tasks);
                Task timeoutTask = Task.Delay(TimeSpan.FromSeconds(60));

                Task winner = await Task.WhenAny(parallelOrchestrations, timeoutTask);
                Assert.AreEqual(parallelOrchestrations, winner);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates the orchestrator's exception handling behavior.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task HandledActivityException(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                // Empty string input should result in ArgumentNullException in the orchestration code.
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.TryCatchLoop), 5);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(15));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(5, JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates the handling of unhandled exceptions generated from orchestrator code.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task UnhandledOrchestrationException(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                // Empty string input should result in ArgumentNullException in the orchestration code.
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Throw), "");
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Failed, status?.OrchestrationStatus);
                Assert.IsTrue(status?.Output.Contains("null") == true);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates the handling of unhandled exceptions generated from activity code.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task UnhandledActivityException(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                string message = "Kah-BOOOOM!!!";
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Throw), message);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Failed, status?.OrchestrationStatus);
                Assert.IsTrue(status?.Output.Contains(message) == true);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// Fan-out/fan-in test which ensures each operation is run only once.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task FanOutToTableStorage(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                int iterations = 100;

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.MapReduceTableStorage), iterations);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(120));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(iterations, int.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// Test which validates the ETW event source.
        /// </summary>
        [TestMethod]
        public void ValidateEventSource()
        {
#if NETCOREAPP
            EventSourceAnalyzer.InspectAll(AnalyticsEventSource.Log);
#else
            try
            {
                EventSourceAnalyzer.InspectAll(AnalyticsEventSource.Log);
            }
            catch (FormatException)
            {
                Assert.Inconclusive("Known issue with .NET Framework, EventSourceAnalyzer, and DateTime parameters");
            }
#endif
        }

        /// <summary>
        /// End-to-end test which validates that orchestrations with <=60KB text message sizes can run successfully.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task SmallTextMessagePayloads(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                // Generate a small random string payload
                const int TargetPayloadSize = 1 * 1024; // 1 KB
                const string Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 {}/<>.-";
                var sb = new StringBuilder();
                var random = new Random();
                while (Encoding.Unicode.GetByteCount(sb.ToString()) < TargetPayloadSize)
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        sb.Append(Chars[random.Next(Chars.Length)]);
                    }
                }

                string message = sb.ToString();
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(message, JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that orchestrations with > 60KB text message sizes can run successfully.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task LargeQueueTextMessagePayloads_BlobUrl(bool enableExtendedSessions)
        {
            // Small enough to be a small table message, but a large queue message
            const int largeMessageSize = 25 * 1024;

            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions, fetchLargeMessages: false))
            {
                await host.StartAsync();

                string message = this.GenerateMediumRandomStringPayload(largeMessageSize, utf8ByteSize: 3, utf16ByteSize: 2).ToString();
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(message, JToken.Parse(status?.Output));
                Assert.AreEqual(message, JToken.Parse(status.Input));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that orchestrations with > 60KB text message sizes can run successfully.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task LargeTableTextMessagePayloads_SizeViolation_BlobUrl(bool enableExtendedSessions)
        {
            // Small enough to be a small queue message, but a large table message due to UTF encoding differences of ASCII characters
            const int largeMessageSize = 32 * 1024;

            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions, fetchLargeMessages: false))
            {
                await host.StartAsync();

                string message = this.GenerateMediumRandomStringPayload(largeMessageSize, utf8ByteSize: 1, utf16ByteSize: 2).ToString();
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                await ValidateBlobUrlAsync(
                    host.TaskHub,
                    client.InstanceId,
                    status?.Input,
                    Encoding.UTF8.GetByteCount(message));
                await ValidateBlobUrlAsync(
                    host.TaskHub,
                    client.InstanceId,
                    status?.Output,
                    Encoding.UTF8.GetByteCount(message));
                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that orchestrations with > 60KB text message sizes can run successfully.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task LargeOverallTextMessagePayloads_BlobUrl(bool enableExtendedSessions)
        {
            const int largeMessageSize = 80 * 1024;

            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions, fetchLargeMessages: false))
            {
                await host.StartAsync();

                string message = this.GenerateMediumRandomStringPayload(numChars: largeMessageSize).ToString();
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                await ValidateBlobUrlAsync(
                    host.TaskHub,
                    client.InstanceId,
                    status?.Input,
                    Encoding.UTF8.GetByteCount(message));
                await ValidateBlobUrlAsync(
                    host.TaskHub,
                    client.InstanceId,
                    status?.Output,
                    Encoding.UTF8.GetByteCount(message));

                Assert.IsTrue(status.Output.EndsWith("-Result.json.gz"));
                Assert.IsTrue(status.Input.EndsWith("-Input.json.gz"));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that orchestrations with > 60KB text message sizes can run successfully.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task LargeTextMessagePayloads_FetchLargeMessages(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions, fetchLargeMessages: true))
            {
                await host.StartAsync();

                string message = this.GenerateMediumRandomStringPayload().ToString();
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(message, JToken.Parse(status?.Input));
                Assert.AreEqual(message, JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that orchestrations with > 60KB text message sizes can run successfully.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task LargeTableTextMessagePayloads_FetchLargeMessages(bool enableExtendedSessions)
        {
            // Small enough to be a small queue message, but a large table message due to UTF encoding differences of ASCII characters
            const int largeMessageSize = 32 * 1024;
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions, fetchLargeMessages: true))
            {
                await host.StartAsync();

                string message = this.GenerateMediumRandomStringPayload(largeMessageSize, utf8ByteSize: 1, utf16ByteSize: 2).ToString();
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(message, JToken.Parse(status?.Input));
                Assert.AreEqual(message, JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that orchestrations with > 60KB of tag data can be run successfully.
        /// </summary>
        [TestMethod]
        public async Task LargeOrchestrationTags()
        {
            const int largeMessageSize = 64 * 1024;
            using TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false, fetchLargeMessages: true);
            await host.StartAsync();

            string bigMessage = this.GenerateMediumRandomStringPayload(largeMessageSize, utf8ByteSize: 1, utf16ByteSize: 2).ToString();
            var bigTags = new Dictionary<string, string> { { "BigTag", bigMessage } };
            var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), "Hello, world!", tags: bigTags);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

            // TODO: Uncomment these assertions as part of https://github.com/Azure/durabletask/issues/840.
            ////Assert.IsNotNull(status?.Tags);
            ////Assert.AreEqual(bigTags.Count, status.Tags.Count);
            ////Assert.IsTrue(bigTags.TryGetValue("BigTag", out string actualMessage));
            ////Assert.AreEqual(bigMessage, actualMessage);

            await host.StopAsync();
        }

        /// <summary>
        /// End-to-end test which validates that orchestrations with > 60KB text message sizes can run successfully.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task NonBlobUriPayload_FetchLargeMessages_RetainsOriginalPayload(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions, fetchLargeMessages: true))
            {
                await host.StartAsync();

                string message = "https://anygivenurl.azurewebsites.net";
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(message, JToken.Parse(status?.Input));
                Assert.AreEqual(message, JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that orchestrations with > 60KB text message sizes can run successfully.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task LargeTextMessagePayloads_FetchLargeMessages_QueryState(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions, fetchLargeMessages: true))
            {
                await host.StartAsync();

                string message = this.GenerateMediumRandomStringPayload().ToString();
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

                //Ensure that orchestration state querying also retrieves messages 
                status = (await client.GetStateAsync(status.OrchestrationInstance.InstanceId)).First();

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(message, JToken.Parse(status?.Input));
                Assert.AreEqual(message, JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that exception messages that are considered valid Urls in the Uri.TryCreate() method
        /// are handled with an additional Uri format check
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task LargeTextMessagePayloads_URIFormatCheck(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions, fetchLargeMessages: true))
            {
                await host.StartAsync();

                string message = this.GenerateMediumRandomStringPayload().ToString();
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.ThrowException), "durabletask.core.exceptions.taskfailedexception: Task failed with an unhandled exception: This is an invalid operation.)");
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

                //Ensure that orchestration state querying also retrieves messages 
                status = (await client.GetStateAsync(status.OrchestrationInstance.InstanceId)).First();

                Assert.AreEqual(OrchestrationStatus.Failed, status?.OrchestrationStatus);
                Assert.IsTrue(status?.Output.Contains("invalid operation") == true);

                await host.StopAsync();
            }
        }

        private StringBuilder GenerateMediumRandomStringPayload(int numChars = 128*1024, short utf8ByteSize = 1, short utf16ByteSize = 2)
        {
            string Chars;
            if (utf16ByteSize != 2 && utf16ByteSize != 4)
            {
                throw new InvalidOperationException($"No characters have byte size {utf16ByteSize} for UTF16");
            }
            else if (utf8ByteSize < 1 || utf8ByteSize > 4)
            {
                throw new InvalidOperationException($"No characters have byte size {utf8ByteSize} for UTF8.");
            }
            else if (utf8ByteSize == 1 && utf16ByteSize == 2)
            {
                // Use a character set that is small for UTF8 and large for UTF16
                // This allows us to produce a smaller string for UTF8 than UTF16.
                Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 {}/<>.";
            }
            else if (utf16ByteSize == 2 && utf8ByteSize == 3)
            {
                // Use a character set that is small for UTF16 and large for UTF8
                // This allows us to produce a smaller string for UTF16 than UTF8.
                Chars = "มันสนุกพี่บุ๋มมันโจ๊ะ";
            }
            else
            {
                throw new InvalidOperationException($"This method has not yet added support for characters of utf8 size {utf8ByteSize} and utf16 size {utf16ByteSize}");
            }

            var random = new Random();
            var sb = new StringBuilder();
            for (int i = 0; i < numChars; i++)
            {
                sb.Append(Chars[random.Next(Chars.Length)]);
            }

            return sb;
        }

        /// <summary>
        /// End-to-end test which validates that orchestrations with > 60KB binary bytes message sizes can run successfully.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task LargeBinaryByteMessagePayloads(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                // Construct byte array from large binary file of size 826KB
                string originalFileName = "large.jpeg";
                string currentDirectory = Directory.GetCurrentDirectory();
                string originalFilePath = Path.Combine(currentDirectory, originalFileName);
                byte[] readBytes = File.ReadAllBytes(originalFilePath);

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.EchoBytes), readBytes);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

                byte[] resultBytes = JObject.Parse(status?.Output).ToObject<byte[]>();
                Assert.IsTrue(readBytes.SequenceEqual(resultBytes));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that orchestrations with > 60KB binary string message sizes can run successfully.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task LargeBinaryStringMessagePayloads(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                // Construct string message from large binary file of size 826KB
                string originalFileName = "large.jpeg";
                string currentDirectory = Directory.GetCurrentDirectory();
                string originalFilePath = Path.Combine(currentDirectory, originalFileName);
                byte[] readBytes = File.ReadAllBytes(originalFilePath);
                string message = Convert.ToBase64String(readBytes);

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

                // Large message payloads may actually get bigger when stored in blob storage.
                string result = JToken.Parse(status?.Output).ToString();
                Assert.AreEqual(message, result);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that a completed singleton instance can be recreated.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task RecreateCompletedInstance(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                string singletonInstanceId = $"HelloSingleton_{Guid.NewGuid():N}";

                var client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.SayHelloWithActivity),
                    input: "One",
                    instanceId: singletonInstanceId);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual("One", JToken.Parse(status?.Input));
                Assert.AreEqual("Hello, One!", JToken.Parse(status?.Output));

                client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.SayHelloWithActivity),
                    input: "Two",
                    instanceId: singletonInstanceId);
                status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual("Two", JToken.Parse(status?.Input));
                Assert.AreEqual("Hello, Two!", JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that a failed singleton instance can be recreated.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task RecreateFailedInstance(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                string singletonInstanceId = $"HelloSingleton_{Guid.NewGuid():N}";

                var client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.SayHelloWithActivity),
                    input: null, // this will cause the orchestration to fail
                    instanceId: singletonInstanceId);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Failed, status?.OrchestrationStatus);

                client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.SayHelloWithActivity),
                    input: "NotNull",
                    instanceId: singletonInstanceId);
                status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual("Hello, NotNull!", JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that a terminated orchestration can be recreated.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task RecreateTerminatedInstance(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                string singletonInstanceId = $"SingletonCounter_{Guid.NewGuid():N}";

                // Using the counter orchestration because it will wait indefinitely for input.
                var client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.Counter),
                    input: -1,
                    instanceId: singletonInstanceId);

                // Need to wait for the instance to start before we can terminate it.
                await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

                await client.TerminateAsync("sayōnara");

                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

                Assert.AreEqual(OrchestrationStatus.Terminated, status?.OrchestrationStatus);
                Assert.AreEqual("-1", status?.Input);
                Assert.AreEqual("sayōnara", status?.Output);

                client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.Counter),
                    input: 0,
                    instanceId: singletonInstanceId);
                status = await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

                Assert.AreEqual(OrchestrationStatus.Running, status?.OrchestrationStatus);
                Assert.AreEqual("0", status?.Input);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that a running orchestration can be recreated.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task RecreateRunningInstance(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(
                enableExtendedSessions,
                extendedSessionTimeoutInSeconds: 15))
            {
                await host.StartAsync();

                string singletonInstanceId = $"SingletonCounter_{DateTime.Now:o}";

                // Using the counter orchestration because it will wait indefinitely for input.
                var client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.Counter),
                    input: 0,
                    instanceId: singletonInstanceId);

                var status = await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

                Assert.AreEqual(OrchestrationStatus.Running, status?.OrchestrationStatus);
                Assert.AreEqual("0", status?.Input);
                Assert.AreEqual(null, status?.Output);

                client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.Counter),
                    input: 99,
                    instanceId: singletonInstanceId);

                // Note that with extended sessions, the startup time may take longer because the dispatcher
                // will wait for the current extended session to expire before the new create message is accepted.
                status = await client.WaitForStartupAsync(TimeSpan.FromSeconds(20));

                Assert.AreEqual(OrchestrationStatus.Running, status?.OrchestrationStatus);
                Assert.AreEqual("99", status?.Input);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that an orchestration can continue processing
        /// even after its extended session has expired.
        /// </summary>
        [TestMethod]
        public async Task ExtendedSessions_SessionTimeout()
        {
            const int SessionTimeoutInseconds = 5;
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(
                enableExtendedSessions: true,
                extendedSessionTimeoutInSeconds: SessionTimeoutInseconds))
            {
                await host.StartAsync();

                string singletonInstanceId = $"SingletonCounter_{DateTime.Now:o}";

                // Using the counter orchestration because it will wait indefinitely for input.
                var client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.Counter),
                    input: 0,
                    instanceId: singletonInstanceId);

                var status = await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

                Assert.AreEqual(OrchestrationStatus.Running, status?.OrchestrationStatus);
                Assert.AreEqual("0", status?.Input);
                Assert.AreEqual(null, status?.Output);

                // Delay long enough for the session to expire
                await Task.Delay(TimeSpan.FromSeconds(SessionTimeoutInseconds + 1));

                await client.RaiseEventAsync("operation", "incr");
                await Task.Delay(TimeSpan.FromSeconds(2));

                // Make sure it's still running and didn't complete early (or fail).
                status = await client.GetStatusAsync();
                Assert.IsTrue(
                    status?.OrchestrationStatus == OrchestrationStatus.Running ||
                    status?.OrchestrationStatus == OrchestrationStatus.ContinuedAsNew);

                // The end message will cause the actor to complete itself.
                await client.RaiseEventAsync("operation", "end");

                status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(1, JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// Tests an orchestration that does two consecutive fan-out, fan-ins.
        /// This is a regression test for https://github.com/Azure/durabletask/issues/241.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task DoubleFanOut(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.DoubleFanOut), null);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                await host.StopAsync();
            }
        }

        private static async Task ValidateBlobUrlAsync(string taskHubName, string instanceId, string value, int originalPayloadSize = 0)
        {
            string sanitizedInstanceId = KeySanitation.EscapePartitionKey(instanceId);

            CloudStorageAccount account = CloudStorageAccount.Parse(TestHelpers.GetTestStorageAccountConnectionString());
            Assert.IsTrue(value.StartsWith(account.BlobStorageUri.PrimaryUri.OriginalString));
            Assert.IsTrue(value.Contains("/" + sanitizedInstanceId + "/"));
            Assert.IsTrue(value.EndsWith(".json.gz"));

            string containerName = $"{taskHubName.ToLowerInvariant()}-largemessages";
            CloudBlobClient client = account.CreateCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference(containerName);
            Assert.IsTrue(await container.ExistsAsync(), $"Blob container {containerName} is expected to exist.");

            await client.GetBlobReferenceFromServerAsync(new Uri(value));
            CloudBlobDirectory instanceDirectory = container.GetDirectoryReference(sanitizedInstanceId);

            string blobName = value.Split('/').Last();
            CloudBlob blob = instanceDirectory.GetBlobReference(blobName);
            Assert.IsTrue(await blob.ExistsAsync(), $"Blob named {blob.Uri} is expected to exist.");

            if (originalPayloadSize > 0)
            {
                await blob.FetchAttributesAsync();
                Assert.IsTrue(blob.Properties.Length < originalPayloadSize, "Blob is expected to be compressed");
            }
        }

        /// <summary>
        /// Tests the behavior of <see cref="SessionAbortedException"/> from orchestrations and activities.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task AbortOrchestrationAndActivity(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                string input = Guid.NewGuid().ToString();
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.AbortSessionOrchestration), input);
                var status = await client.WaitForCompletionAsync(StandardTimeout);

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.IsNotNull(status.Output);
                Assert.AreEqual("True", JToken.Parse(status.Output));
                await host.StopAsync();
            }
        }

                /// <summary>
        /// Validates scheduled starts, ensuring they are executed according to defined start date time
        /// </summary>
        /// <param name="enableExtendedSessions"></param>
        /// <returns></returns>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task ScheduledStart_Inline(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                var expectedStartTime = DateTime.UtcNow.AddSeconds(30);
                var clientStartingIn30Seconds = await host.StartOrchestrationAsync(typeof(Orchestrations.CurrentTimeInline), "Current Time!", startAt: expectedStartTime);
                var clientStartingNow = await host.StartOrchestrationAsync(typeof(Orchestrations.CurrentTimeInline), "Current Time!");

                var statusStartingNow = clientStartingNow.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                var statusStartingIn30Seconds = clientStartingIn30Seconds.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                await Task.WhenAll(statusStartingNow, statusStartingIn30Seconds);

                Assert.AreEqual(OrchestrationStatus.Completed, statusStartingNow.Result?.OrchestrationStatus);
                Assert.AreEqual("Current Time!", JToken.Parse(statusStartingNow.Result?.Input));
                Assert.IsNull(statusStartingNow.Result.ScheduledStartTime);

                Assert.AreEqual(OrchestrationStatus.Completed, statusStartingIn30Seconds.Result?.OrchestrationStatus);
                Assert.AreEqual("Current Time!", JToken.Parse(statusStartingIn30Seconds.Result?.Input));
                Assert.AreEqual(expectedStartTime, statusStartingIn30Seconds.Result.ScheduledStartTime);

                var startNowResult = (DateTime)JToken.Parse(statusStartingNow.Result?.Output);
                var startIn30SecondsResult = (DateTime)JToken.Parse(statusStartingIn30Seconds.Result?.Output);

                Assert.IsTrue(startIn30SecondsResult > startNowResult);
                Assert.IsTrue(startIn30SecondsResult >= expectedStartTime);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// Validates scheduled starts, ensuring they are executed according to defined start date time
        /// </summary>
        /// <param name="enableExtendedSessions"></param>
        /// <returns></returns>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task ScheduledStart_Activity(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                var expectedStartTime = DateTime.UtcNow.AddSeconds(30);
                var clientStartingIn30Seconds = await host.StartOrchestrationAsync(typeof(Orchestrations.CurrentTimeActivity), "Current Time!", startAt: expectedStartTime);
                var clientStartingNow = await host.StartOrchestrationAsync(typeof(Orchestrations.CurrentTimeActivity), "Current Time!");

                var statusStartingNow = clientStartingNow.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                var statusStartingIn30Seconds = clientStartingIn30Seconds.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                await Task.WhenAll(statusStartingNow, statusStartingIn30Seconds);

                Assert.AreEqual(OrchestrationStatus.Completed, statusStartingNow.Result?.OrchestrationStatus);
                Assert.AreEqual("Current Time!", JToken.Parse(statusStartingNow.Result?.Input));
                Assert.IsNull(statusStartingNow.Result.ScheduledStartTime);

                Assert.AreEqual(OrchestrationStatus.Completed, statusStartingIn30Seconds.Result?.OrchestrationStatus);
                Assert.AreEqual("Current Time!", JToken.Parse(statusStartingIn30Seconds.Result?.Input));
                Assert.AreEqual(expectedStartTime, statusStartingIn30Seconds.Result.ScheduledStartTime);

                var startNowResult = (DateTime)JToken.Parse(statusStartingNow.Result?.Output);
                var startIn30SecondsResult = (DateTime)JToken.Parse(statusStartingIn30Seconds.Result?.Output);

                Assert.IsTrue(startIn30SecondsResult > startNowResult);
                Assert.IsTrue(startIn30SecondsResult >= expectedStartTime);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// Validates scheduled starts, ensuring they are executed according to defined start date time
        /// </summary>
        /// <param name="enableExtendedSessions"></param>
        /// <returns></returns>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task ScheduledStart_Activity_GetStatus_Returns_ScheduledStart(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                var expectedStartTime = DateTime.UtcNow.AddSeconds(30);
                var clientStartingIn30Seconds = await host.StartOrchestrationAsync(typeof(Orchestrations.DelayedCurrentTimeActivity), "Delayed Current Time!", startAt: expectedStartTime);
                var clientStartingNow = await host.StartOrchestrationAsync(typeof(Orchestrations.DelayedCurrentTimeActivity), "Delayed Current Time!");

                var statusStartingIn30Seconds = await clientStartingIn30Seconds.GetStatusAsync();
                Assert.IsNotNull(statusStartingIn30Seconds.ScheduledStartTime);
                Assert.AreEqual(expectedStartTime, statusStartingIn30Seconds.ScheduledStartTime);

                var statusStartingNow = await clientStartingNow.GetStatusAsync();
                Assert.IsNull(statusStartingNow.ScheduledStartTime);

                await Task.WhenAll(
                    clientStartingNow.WaitForCompletionAsync(TimeSpan.FromSeconds(35)),
                    clientStartingIn30Seconds.WaitForCompletionAsync(TimeSpan.FromSeconds(65))
                    );

                await host.StopAsync();
            }
        }

        static class Orchestrations
        {
            internal class SayHelloInline : TaskOrchestration<string, string>
            {
                public override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    return Task.FromResult($"Hello, {input}!");
                }
            }

            [KnownType(typeof(Activities.Hello))]
            internal class SayHelloWithActivity : TaskOrchestration<string, string>
            {
                public override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    return context.ScheduleTask<string>(typeof(Activities.Hello), input);
                }
            }

            [KnownType(typeof(Activities.HelloFailActivity))]
            internal class SayHelloWithActivityFail : TaskOrchestration<string, string>
            {
                public override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    return context.ScheduleTask<string>(typeof(Activities.HelloFailActivity), input);
                }
            }

            [KnownType(typeof(Activities.Multiply))]
            internal class Factorial : TaskOrchestration<long, int>
            {
                public override async Task<long> RunTask(OrchestrationContext context, int n)
                {
                    long result = 1;
                    for (int i = 1; i <= n; i++)
                    {
                        result = await (context.ScheduleTask<long>(typeof(Activities.Multiply), new[] { result, i }));
                    }
                    return result;
                }
            }

            [KnownType(typeof(Activities.Multiply))]
            internal class FactorialFail : TaskOrchestration<long, int>
            {
                public static bool ShouldFail = true;
                public override async Task<long> RunTask(OrchestrationContext context, int n)
                {
                    long result = 1;
                    for (int i = 1; i <= n; i++)
                    {
                        result = await (context.ScheduleTask<long>(typeof(Activities.Multiply), new[] { result, i }));
                    }
                    if (ShouldFail)
                    {
                        throw new Exception("Simulating a transient, unhandled exception");
                    }
                    return result;
                }
            }

            [KnownType(typeof(Activities.Multiply))]
            internal class FactorialOrchestratorFail : TaskOrchestration<long, int>
            {
                public static bool ShouldFail = true;
                public override async Task<long> RunTask(OrchestrationContext context, int n)
                {
                    long result = 1;
                    for (int i = 1; i <= n; i++)
                    {
                        result = await (context.ScheduleTask<long>(typeof(Activities.Multiply), new[] { result, i }));
                    }
                    if (ShouldFail)
                    {
                        throw new Exception("Simulating a transient, unhandled exception");
                    }
                    return result;
                }
            }

            [KnownType(typeof(Activities.MultiplyMultipleActivityFail))]
            internal class FactorialMultipleActivityFail : TaskOrchestration<long, int>
            {
                public override async Task<long> RunTask(OrchestrationContext context, int n)
                {
                    long result = 1;
                    for (int i = 1; i <= n; i++)
                    {
                        result = await (context.ScheduleTask<long>(typeof(Activities.MultiplyMultipleActivityFail), new[] { result, i }));
                    }

                    return result;
                }
            }

            [KnownType(typeof(Activities.Multiply))]
            internal class FactorialNoReplay : Factorial
            {
                public override Task<long> RunTask(OrchestrationContext context, int n)
                {
                    if (context.IsReplaying)
                    {
                        throw new Exception("Replaying is forbidden in this test.");
                    }

                    return base.RunTask(context, n);
                }
            }

            internal class LongRunningOrchestrator : TaskOrchestration<string, string>
            {
                public override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(10));
                    if (input == "0")
                    {
                        context.ContinueAsNew("1");
                        return Task.FromResult("");
                    }
                    else
                    {
                        return Task.FromResult("ok");
                    }
                }
            }

            [KnownType(typeof(Activities.GetFileList))]
            [KnownType(typeof(Activities.GetFileSize))]
            internal class DiskUsage : TaskOrchestration<long, string>
            {
                public override async Task<long> RunTask(OrchestrationContext context, string directory)
                {
                    string[] files = await context.ScheduleTask<string[]>(typeof(Activities.GetFileList), directory);

                    var tasks = new Task<long>[files.Length];
                    for (int i = 0; i < files.Length; i++)
                    {
                        tasks[i] = context.ScheduleTask<long>(typeof(Activities.GetFileSize), files[i]);
                    }

                    await Task.WhenAll(tasks);

                    long totalBytes = tasks.Sum(t => t.Result);
                    return totalBytes;
                }
            }

            [KnownType(typeof(Activities.Hello))]
            internal class FanOutFanIn : TaskOrchestration<string, int>
            {
                public override async Task<string> RunTask(OrchestrationContext context, int parallelTasks)
                {
                    var tasks = new Task[parallelTasks];
                    for (int i = 0; i < tasks.Length; i++)
                    {
                        tasks[i] = context.ScheduleTask<string>(typeof(Activities.Hello), i.ToString("000"));
                    }

                    await Task.WhenAll(tasks);

                    return "Done";
                }
            }

            [KnownType(typeof(Activities.HelloFailFanOut))]
            internal class FanOutFanInRewind : TaskOrchestration<string, int>
            {
                public override async Task<string> RunTask(OrchestrationContext context, int parallelTasks)
                {
                    var tasks = new Task[parallelTasks];
                    for (int i = 0; i < tasks.Length; i++)
                    {
                        tasks[i] = context.ScheduleTask<string>(typeof(Activities.HelloFailFanOut), i.ToString("000"));
                    }

                    await Task.WhenAll(tasks);

                    return "Done";
                }
            }

            [KnownType(typeof(Activities.Echo))]
            internal class SemiLargePayloadFanOutFanIn : TaskOrchestration<string, int>
            {
                static readonly string Some50KBPayload = new string('x', 25 * 1024); // Assumes UTF-16 encoding
                static readonly string Some16KBPayload = new string('x', 8 * 1024); // Assumes UTF-16 encoding

                public override async Task<string> RunTask(OrchestrationContext context, int parallelTasks)
                {
                    var tasks = new Task[parallelTasks];
                    for (int i = 0; i < tasks.Length; i++)
                    {
                        tasks[i] = context.ScheduleTask<string>(typeof(Activities.Echo), Some50KBPayload);
                    }

                    await Task.WhenAll(tasks);

                    return "Done";
                }

                public override string GetStatus()
                {
                    return Some16KBPayload;
                }
            }

            [KnownType(typeof(Orchestrations.ParentWorkflowSubOrchestrationFail))]
            [KnownType(typeof(Activities.Hello))]
            public class ChildWorkflowSubOrchestrationFail : TaskOrchestration<string, int>
            {
                public static bool ShouldFail1 = true;
                public static bool ShouldFail2 = true;
                public override async Task<string> RunTask(OrchestrationContext context, int input)
                {
                    if (ShouldFail1 || ShouldFail2)
                    {
                        throw new Exception("Simulating sub-orchestration failure...");
                    }
                    var result = await context.ScheduleTask<string>(typeof(Activities.Hello), input);
                    return result;
                }
            }

            [KnownType(typeof(Orchestrations.ParentWorkflowSubOrchestrationActivityFail))]
            [KnownType(typeof(Activities.HelloFailSubOrchestrationActivity))]
            public class ChildWorkflowSubOrchestrationActivityFail : TaskOrchestration<string, int>
            {
                public override async Task<string> RunTask(OrchestrationContext context, int input)
                {
                    var result = await context.ScheduleTask<string>(typeof(Activities.HelloFailSubOrchestrationActivity), input);
                    return result;
                }
            }

            [KnownType(typeof(Orchestrations.GrandparentWorkflowNestedActivityFail))]
            [KnownType(typeof(Orchestrations.ParentWorkflowNestedActivityFail))]
            [KnownType(typeof(Activities.HelloFailNestedSuborchestration))]
            public class ChildWorkflowNestedActivityFail : TaskOrchestration<string, int>
            {
                public override async Task<string> RunTask(OrchestrationContext context, int input)
                {
                    var result = await context.ScheduleTask<string>(typeof(Activities.HelloFailNestedSuborchestration), input);
                    return result;
                }
            }

            [KnownType(typeof(Orchestrations.ChildWorkflowSubOrchestrationFail))]
            [KnownType(typeof(Activities.Hello))]
            public class ParentWorkflowSubOrchestrationFail : TaskOrchestration<string, bool>
            {
                public static string Result;
                public override async Task<string> RunTask(OrchestrationContext context, bool waitForCompletion)
                {
                    var results = new Task<string>[2];
                    for (int i = 0; i < 2; i++)
                    {
                        Task<string> r = context.CreateSubOrchestrationInstance<string>(typeof(Orchestrations.ChildWorkflowSubOrchestrationFail), i);
                        if (waitForCompletion)
                        {
                            await r;
                        }
                        results[i] = r;
                    }

                    string[] data = await Task.WhenAll(results);
                    Result = string.Concat(data);
                    return Result;
                }
            }


            [KnownType(typeof(Orchestrations.GrandparentWorkflowNestedActivityFail))]
            [KnownType(typeof(Orchestrations.ChildWorkflowNestedActivityFail))]
            [KnownType(typeof(Activities.HelloFailNestedSuborchestration))]
            public class ParentWorkflowNestedActivityFail : TaskOrchestration<string, bool>
            {
                public static string Result;
                public override async Task<string> RunTask(OrchestrationContext context, bool waitForCompletion)
                {
                    var results = new Task<string>[2];
                    for (int i = 0; i < 2; i++)
                    {
                        Task<string> r = context.CreateSubOrchestrationInstance<string>(typeof(Orchestrations.ChildWorkflowNestedActivityFail), i);
                        if (waitForCompletion)
                        {
                            await r;
                        }
                        results[i] = r;
                    }

                    string[] data = await Task.WhenAll(results);
                    Result = string.Concat(data);
                    return Result;
                }
            }

            [KnownType(typeof(Orchestrations.ChildWorkflowSubOrchestrationActivityFail))]
            [KnownType(typeof(Activities.HelloFailSubOrchestrationActivity))]
            public class ParentWorkflowSubOrchestrationActivityFail : TaskOrchestration<string, bool>
            {
                public static string Result;
                public override async Task<string> RunTask(OrchestrationContext context, bool waitForCompletion)
                {
                    var results = new Task<string>[2];
                    for (int i = 0; i < 2; i++)
                    {
                        Task<string> r = context.CreateSubOrchestrationInstance<string>(typeof(Orchestrations.ChildWorkflowSubOrchestrationActivityFail), i);
                        if (waitForCompletion)
                        {
                            await r;
                        }
                        results[i] = r;
                    }

                    string[] data = await Task.WhenAll(results);
                    Result = string.Concat(data);
                    return Result;
                }
            }

            [KnownType(typeof(Orchestrations.ParentWorkflowNestedActivityFail))]
            [KnownType(typeof(Orchestrations.ChildWorkflowNestedActivityFail))]
            [KnownType(typeof(Activities.HelloFailNestedSuborchestration))]
            public class GrandparentWorkflowNestedActivityFail : TaskOrchestration<string, bool>
            {
                public static string Result;
                public override async Task<string> RunTask(OrchestrationContext context, bool waitForCompletion)
                {
                    var results = new Task<string>[2];
                    for (int i = 0; i < 2; i++)
                    {
                        Task<string> r = context.CreateSubOrchestrationInstance<string>(typeof(Orchestrations.ParentWorkflowNestedActivityFail), i);
                        if (waitForCompletion)
                        {
                            await r;
                        }
                        results[i] = r;
                    }

                    string[] data = await Task.WhenAll(results);
                    Result = string.Concat(data);
                    return Result;
                }
            }

            internal class Counter : TaskOrchestration<int, int>
            {
                TaskCompletionSource<string> waitForOperationHandle;

                public override async Task<int> RunTask(OrchestrationContext context, int currentValue)
                {
                    string operation = await this.WaitForOperation();

                    bool done = false;
                    switch (operation?.ToLowerInvariant())
                    {
                        case "incr":
                            currentValue++;
                            break;
                        case "decr":
                            currentValue--;
                            break;
                        case "end":
                            done = true;
                            break;
                    }

                    if (!done)
                    {
                        context.ContinueAsNew(currentValue);
                    }

                    return currentValue;

                }

                async Task<string> WaitForOperation()
                {
                    this.waitForOperationHandle = new TaskCompletionSource<string>();
                    string operation = await this.waitForOperationHandle.Task;
                    this.waitForOperationHandle = null;
                    return operation;
                }

                public override void OnEvent(OrchestrationContext context, string name, string input)
                {
                    Assert.AreEqual("operation", name, true, "Unknown signal recieved...");
                    if (this.waitForOperationHandle != null)
                    {
                        this.waitForOperationHandle.SetResult(input);
                    }
                }
            }           

            [KnownType(typeof(Entities.Counter))]
            public sealed class CallCounterEntity : TaskOrchestration<string, EntityId>
            {
                public override async Task<string> RunTask(OrchestrationContext context, EntityId entityId)
                {
                    await context.CallEntityAsync<int>(entityId, "set", 33);
                    int result = await context.CallEntityAsync<int>(entityId, "get");

                    if (result == 33)
                    {
                        return "OK";
                    }
                    else
                    {
                        return $"wrong result: {result} instead of 33";
                    }
                }
            }

            [KnownType(typeof(Entities.Counter))]
            [KnownType(typeof(Entities.Relay))]
            public sealed class PollCounterEntity : TaskOrchestration<string, EntityId>
            {
                public override async Task<string> RunTask(OrchestrationContext ctx, EntityId entityId)
                {
                    while (true)
                    {
                        var result = await ctx.CallEntityAsync<int>(entityId, "get");

                        if (result != 0)
                        {
                            if (result == 1)
                            {
                                return "ok";
                            }
                            else
                            {
                                return $"fail: wrong entity state: expected 1, got {result}";
                            }
                        }

                        await ctx.CreateTimer(DateTime.UtcNow + TimeSpan.FromSeconds(1), CancellationToken.None);
                    }
                }
            }

            [KnownType(typeof(Entities.Counter))]
            public sealed class CreateEmptyEntities : TaskOrchestration<string, EntityId[]>
            {
                public override async Task<string> RunTask(OrchestrationContext context, EntityId[] entityIds)
                {
                    var tasks = new List<Task>();
                    for (int i = 0; i < entityIds.Length; i++)
                    {
                        tasks.Add(context.CallEntityAsync(entityIds[i], "delete"));
                    }

                    await Task.WhenAll(tasks);
                    return "ok";
                }
            }

            [KnownType(typeof(Entities.Counter))]
            [KnownType(typeof(Activities.Hello))]
            public sealed class LockThenFailReplay : TaskOrchestration<string, (EntityId entityId, bool createNondeterminismFailure)>
            {
                public override async Task<string> RunTask(OrchestrationContext context, (EntityId entityId, bool createNondeterminismFailure) input)
                {
                    if (!(input.createNondeterminismFailure && context.IsReplaying))
                    {
                        await context.LockEntitiesAsync(input.entityId);

                        await context.ScheduleTask<string>(typeof(Activities.Hello), "Tokyo");
                    }

                    return "ok";
                }
            }

            [KnownType(typeof(Entities.Counter))]
            public sealed class LockedTransfer : TaskOrchestration<(int, int), (EntityId, EntityId)>
            {
                public override async Task<(int, int)> RunTask(OrchestrationContext ctx, (EntityId, EntityId) input)
                {
                    var (from, to) = input;

                    if (from.Equals(to))
                    {
                        throw new ArgumentException("from and to must be distinct");
                    }

                    if (ctx.IsInsideCriticalSection)
                    {
                        throw new Exception("test failed: lock context is incorrect");
                    }

                    int fromBalance;
                    int toBalance;

                    using (await ctx.LockEntitiesAsync(from, to))
                    {
                        if (!ctx.IsInsideCriticalSection)
                        {
                            throw new Exception("test failed: lock context is incorrect, must be in critical section");
                        }

                        var availableEntities = ctx.GetAvailableEntities().ToList();

                        if (availableEntities.Count != 2 || !availableEntities.Contains(from) || !availableEntities.Contains(to))
                        {
                            throw new Exception("test failed: lock context is incorrect: available locks do not match");
                        }

                        // read balances in parallel
                        var t1 = ctx.CallEntityAsync<int>(from, "get");

                        availableEntities = ctx.GetAvailableEntities().ToList();
                        if (availableEntities.Count != 1 || !availableEntities.Contains(to))
                        {
                            throw new Exception("test failed: lock context is incorrect: available locks wrong");
                        }

                        var t2 = ctx.CallEntityAsync<int>(to, "get");

                        availableEntities = ctx.GetAvailableEntities().ToList();
                        if (availableEntities.Count != 0)
                        {
                            throw new Exception("test failed: lock context is incorrect: available locks wrong");
                        }

                        fromBalance = await t1;
                        toBalance = await t2;

                        availableEntities = ctx.GetAvailableEntities().ToList();
                        if (availableEntities.Count != 2 || !availableEntities.Contains(from) || !availableEntities.Contains(to))
                        {
                            throw new Exception("test failed: lock context is incorrect: available locks do not match");
                        }

                        // modify
                        fromBalance--;
                        toBalance++;

                        // write balances in parallel
                        var t3 = ctx.CallEntityAsync(from, "set", fromBalance);
                        var t4 = ctx.CallEntityAsync(to, "set", toBalance);
                        await t3;
                        await t4;
                    }

                    if (ctx.IsInsideCriticalSection)
                    {
                        throw new Exception("test failed: lock context is incorrect");
                    }

                    return (fromBalance, toBalance);
                }
            }


            [KnownType(typeof(Entities.StringStore))]
            [KnownType(typeof(Entities.StringStore2))]
            public sealed class SignalAndCallStringStore : TaskOrchestration<string, EntityId>
            {
                public override async Task<string> RunTask(OrchestrationContext ctx, EntityId entity)
                {
                    ctx.SignalEntity(entity, "set", "333");

                    var result = await ctx.CallEntityAsync<string>(entity, "get");

                    if (result != "333")
                    {
                        return $"fail: wrong entity state: expected 333, got {result}";
                    }

                    return "ok";
                }
            }

            [KnownType(typeof(Entities.StringStore))]
            [KnownType(typeof(Entities.StringStore2))]
            public sealed class CallAndDeleteStringStore : TaskOrchestration<string, EntityId>
            {
                public override async Task<string> RunTask(OrchestrationContext ctx, EntityId entity)
                {
                    await ctx.CallEntityAsync(entity, "set", "333");

                    await ctx.CallEntityAsync<string>(entity, "delete");

                    return "ok";
                }
            }

            [KnownType(typeof(Orchestrations.DelayedSignal))]
            [KnownType(typeof(Entities.Launcher))]
            public sealed class LaunchOrchestrationFromEntity : TaskOrchestration<string, EntityId>
            {
                public override async Task<string> RunTask(OrchestrationContext ctx, EntityId entityId)
                {
                    await ctx.CallEntityAsync(entityId, "launch", "hello");

                    while (true)
                    {
                        var orchestrationId = await ctx.CallEntityAsync<string>(entityId, "get");

                        if (orchestrationId != null)
                        {
                            return orchestrationId;
                        }

                        await ctx.CreateTimer(DateTime.UtcNow + TimeSpan.FromSeconds(1), CancellationToken.None);
                    }
                }
            }

            public sealed class DelayedSignal : TaskOrchestration<string, EntityId>
            {
                public override async Task<string> RunTask(OrchestrationContext ctx, EntityId entityId)
                {
                    await ctx.CreateTimer(ctx.CurrentUtcDateTime + TimeSpan.FromSeconds(.2), CancellationToken.None);

                    ctx.SignalEntity(entityId, "done");

                    return "";
                }
            }

            [KnownType(typeof(Entities.FaultyEntityWithRollback))]
            [KnownType(typeof(Entities.FaultyEntityWithoutRollback))]
            public sealed class CallFaultyEntity : TaskOrchestration<string, (EntityId, bool, ErrorPropagationMode)>
            {              
                public override async Task<string> RunTask(OrchestrationContext ctx, (EntityId, bool, ErrorPropagationMode) input)
                {
                    (EntityId entityId, bool rollbackOnException, ErrorPropagationMode mode) = input;

                    async Task ExpectException<TInner>(Task t)
                    {
                        try
                        {
                            await t;
                            Assert.IsTrue(false, "expected exception");
                        }
                        catch (OperationFailedException e) 
                        {
                            if (mode == ErrorPropagationMode.SerializeExceptions)
                            {
                                Assert.IsTrue(e.InnerException != null && e.InnerException is TInner);
                                Assert.IsTrue(e.FailureDetails == null);
                            }
                            else // check that we received the failure details instead of the exception
                            {
                                Assert.IsTrue(e.InnerException == null);
                                Assert.IsTrue(e.FailureDetails != null);
                                Assert.IsTrue(e.FailureDetails.ErrorType == typeof(TInner).FullName);
                            }
                        }
                        catch (Exception e)
                        {
                            Assert.IsTrue(false, $"wrong exception: {e}");
                        }
                    }

                    Task<int> Get() => ctx.CallEntityAsync<int>(entityId, "Get");
                    Task Set(int val) => ctx.CallEntityAsync(entityId, "Set", val);
                    Task SetThenThrow(int val) => ctx.CallEntityAsync(entityId, "SetThenThrow", val);
                    Task SetToUnserializable() => ctx.CallEntityAsync(entityId, "SetToUnserializable");
                    Task SetToUnDeserializable() => ctx.CallEntityAsync(entityId, "SetToUnDeserializable");
                    Task DeleteThenThrow () => ctx.CallEntityAsync(entityId, "DeleteThenThrow"); 
                    Task Delete() => ctx.CallEntityAsync(entityId, "Delete");
                    

                    async Task TestRollbackToNonexistent()
                    {
                        Assert.IsFalse(await ctx.CallEntityAsync<bool>(entityId, "exists"));
                        await ExpectException<EntitySchedulerException>(SetToUnserializable());
                        Assert.IsFalse(await ctx.CallEntityAsync<bool>(entityId, "exists"));
                    }

                    async Task TestNondeserializableState()
                    {
                        await SetToUnDeserializable();
                        Assert.IsTrue(await ctx.CallEntityAsync<bool>(entityId, "exists"));
                        await ExpectException<EntitySchedulerException>(Get());
                        await ctx.CallEntityAsync<bool>(entityId, "deletewithoutreading");
                        Assert.IsFalse(await ctx.CallEntityAsync<bool>(entityId, "exists"));
                    }

                    async Task TestSecondOperationRollback()
                    {
                        await Set(3);
                        Assert.AreEqual(3, await Get());
                        await ExpectException<Entities.FaultyEntity.SerializableKaboom>(SetThenThrow(333));
                        if (rollbackOnException)
                        {
                            Assert.AreEqual(3, await Get());
                        }
                        else
                        {
                            Assert.AreEqual(333, await Get());
                        }
                        await Delete();
                        Assert.IsFalse(await ctx.CallEntityAsync<bool>(entityId, "exists"));
                    }

                    async Task TestDeleteOperationRollback()
                    {
                        await Set(3);
                        Assert.AreEqual(3, await Get());
                        await ExpectException<Entities.FaultyEntity.SerializableKaboom>(SetThenThrow(333));
                        if (rollbackOnException)
                        {
                            Assert.AreEqual(3, await Get());
                        }
                        else
                        {
                            Assert.AreEqual(333, await Get());
                        }
                        await ExpectException<Entities.FaultyEntity.SerializableKaboom>(DeleteThenThrow());
                        if (rollbackOnException)
                        {
                            Assert.AreEqual(3, await Get());
                        }
                        else
                        {
                            Assert.AreEqual(0, await Get());
                        }
                        await Delete();
                        Assert.IsFalse(await ctx.CallEntityAsync<bool>(entityId, "exists"));
                    }

                    async Task TestFirstOperationRollback()
                    {
                        await ExpectException<Entities.FaultyEntity.SerializableKaboom>(SetThenThrow(333));

                        if (!rollbackOnException)
                        { 
                            Assert.IsTrue(await ctx.CallEntityAsync<bool>(entityId, "exists"));
                            await Delete();
                        }
                        Assert.IsFalse(await ctx.CallEntityAsync<bool>(entityId, "exists"));
                    }

                    // we use this utility function to try to enforce that a bunch of signals is delivered as a single batch.
                    // This is required for some of the tests here to work, since the batching affects the entity state management.
                    // The "enforcement" mechanism we use is not 100% failsafe (it still makes timing assumptions about the provider)
                    // but it should be more reliable than the original version of this test which failed quite frequently, as it was
                    // simply assuming that signals that are sent at the same time are always processed as a batch.
                    async Task ProcessAllSignalsInSingleBatch(Action sendSignals)
                    {
                        // first issue a signal that, when delivered, keeps the entity busy for a second
                        ctx.SignalEntity(entityId, "delay", 1);

                        // we now need to yield briefly so that the delay signal is sent before the others
                        await ctx.CreateTimer(ctx.CurrentUtcDateTime + TimeSpan.FromMilliseconds(1), CancellationToken.None);

                        // now send the signals in the batch. These should all arrive and get queued (inside the storage provider)
                        // while the entity is executing the delay operation. Therefore, after the delay operation finishes,
                        // all of the signals are processed in a single batch.
                        sendSignals();
                    }

                    async Task TestRollbackInBatch1()
                    {
                        await ProcessAllSignalsInSingleBatch(() =>
                        {
                            ctx.SignalEntity(entityId, "Set", 42);
                            ctx.SignalEntity(entityId, "SetThenThrow", 333);
                            ctx.SignalEntity(entityId, "DeleteThenThrow");
                        });

                        if (rollbackOnException)
                        {
                            Assert.AreEqual(42, await Get());
                        }
                        else
                        {
                            Assert.IsFalse(await ctx.CallEntityAsync<bool>(entityId, "exists"));
                            ctx.SignalEntity(entityId, "Set", 42);
                        }
                    }

                    async Task TestRollbackInBatch2()
                    {
                        await ProcessAllSignalsInSingleBatch(() =>
                        {
                            ctx.SignalEntity(entityId, "Get");
                            ctx.SignalEntity(entityId, "Set", 42);
                            ctx.SignalEntity(entityId, "Delete");
                            ctx.SignalEntity(entityId, "Set", 43);
                            ctx.SignalEntity(entityId, "DeleteThenThrow");
                        });

                        if (rollbackOnException)
                        {
                            Assert.AreEqual(43, await Get());
                        }
                        else
                        {
                            Assert.IsFalse(await ctx.CallEntityAsync<bool>(entityId, "exists"));
                        }
                    }

                    async Task TestRollbackInBatch3()
                    {
                        Task<int> getTask = null;

                        await ProcessAllSignalsInSingleBatch(() =>
                        {
                            ctx.SignalEntity(entityId, "Set", 55);
                            ctx.SignalEntity(entityId, "SetToUnserializable");
                            getTask = ctx.CallEntityAsync<int>(entityId, "Get");
                        });

                        if (rollbackOnException)
                        {
                            Assert.AreEqual(55, await getTask);
                            await Delete();
                        }
                        else
                        {
                            await ExpectException<EntitySchedulerException>(getTask);
                            await ctx.CallEntityAsync<bool>(entityId, "deletewithoutreading");
                        }
                        Assert.IsFalse(await ctx.CallEntityAsync<bool>(entityId, "exists"));
                    }

                    async Task TestRollbackInBatch4()
                    {
                        Task<int> getTask = null; 
                        
                        await ProcessAllSignalsInSingleBatch(() =>
                        {
                            ctx.SignalEntity(entityId, "Set", 56);
                            ctx.SignalEntity(entityId, "SetToUnDeserializable");
                            ctx.SignalEntity(entityId, "SetThenThrow", 999);
                            getTask = ctx.CallEntityAsync<int>(entityId, "Get");
                        });

                        if (rollbackOnException)
                        {
                            // we rolled back to an un-deserializable state
                            await ExpectException<EntitySchedulerException>(getTask);
                        }
                        else
                        {
                            // we don't roll back the 999 when throwing
                            Assert.AreEqual(999, await getTask);
                        }

                        await ctx.CallEntityAsync<bool>(entityId, "deletewithoutreading");
                    }


                    async Task TestRollbackInBatch5()
                    {                      
                        await ProcessAllSignalsInSingleBatch(() =>
                        {
                            ctx.SignalEntity(entityId, "Set", 1);
                            ctx.SignalEntity(entityId, "Delete");
                            ctx.SignalEntity(entityId, "Set", 2);
                            ctx.SignalEntity(entityId, "Delete");
                            ctx.SignalEntity(entityId, "SetThenThrow", 3);
                        });

                        if (rollbackOnException)
                        {
                            Assert.IsFalse(await ctx.CallEntityAsync<bool>(entityId, "exists"));
                        }
                        else
                        {
                            Assert.AreEqual(3, await Get()); // not rolled back
                        }
                    }

                    try
                    {
                        await TestRollbackToNonexistent();
                        await TestNondeserializableState();
                        await TestSecondOperationRollback();
                        await TestDeleteOperationRollback();
                        await TestFirstOperationRollback();

                        await TestRollbackInBatch1();
                        await TestRollbackInBatch2();
                        await TestRollbackInBatch3();
                        await TestRollbackInBatch4();
                        await TestRollbackInBatch5();

                        return "ok";
                    }
                    catch (Exception e)
                    {
                        return e.ToString();
                    }
                }
            }

            internal class CharacterCounter : TaskOrchestration<Tuple<string, int>, Tuple<string, int>>
            {
                TaskCompletionSource<string> waitForOperationHandle;

                public override async Task<Tuple<string, int>> RunTask(OrchestrationContext context, Tuple<string, int> inputData)
                {
                    string operation = await this.WaitForOperation();
                    bool done = false;
                    switch (operation?.ToLowerInvariant())
                    {
                        case "double":
                            inputData = new Tuple<string, int>(
                                $"{inputData.Item1}{new string(inputData.Item1.Reverse().ToArray())}",
                                inputData.Item2 * 2);
                            break;
                        case "end":
                            done = true;
                            break;
                    }

                    if (!done)
                    {
                        context.ContinueAsNew(inputData);
                    }

                    return inputData;
                }

                async Task<string> WaitForOperation()
                {
                    this.waitForOperationHandle = new TaskCompletionSource<string>();
                    string operation = await this.waitForOperationHandle.Task;
                    this.waitForOperationHandle = null;
                    return operation;
                }

                public override void OnEvent(OrchestrationContext context, string name, string input)
                {
                    Assert.AreEqual("operation", name, true, "Unknown signal recieved...");
                    if (this.waitForOperationHandle != null)
                    {
                        this.waitForOperationHandle.SetResult(input);
                    }
                }
            }

            internal class Approval : TaskOrchestration<string, TimeSpan, bool, string>
            {
                TaskCompletionSource<bool> waitForApprovalHandle;
                public static bool shouldFail = false;

                public override async Task<string> RunTask(OrchestrationContext context, TimeSpan timeout)
                {
                    DateTime deadline = context.CurrentUtcDateTime.Add(timeout);

                    using (var cts = new CancellationTokenSource())
                    {
                        Task<bool> approvalTask = this.GetWaitForApprovalTask();
                        Task timeoutTask = context.CreateTimer(deadline, cts.Token);

                        if (shouldFail)
                        {
                            throw new Exception("Simulating unhanded error exception");
                        }

                        if (approvalTask == await Task.WhenAny(approvalTask, timeoutTask))
                        {
                            // The timer must be cancelled or fired in order for the orchestration to complete.
                            cts.Cancel();

                            bool approved = approvalTask.Result;
                            return approved ? "Approved" : "Rejected";
                        }
                        else
                        {
                            return "Expired";
                        }
                    }
                }

                async Task<bool> GetWaitForApprovalTask()
                {
                    this.waitForApprovalHandle = new TaskCompletionSource<bool>();
                    bool approvalResult = await this.waitForApprovalHandle.Task;
                    this.waitForApprovalHandle = null;
                    return approvalResult;
                }

                public override void OnEvent(OrchestrationContext context, string name, bool approvalResult)
                {
                    Assert.AreEqual("approval", name, true, "Unknown signal recieved...");
                    if (this.waitForApprovalHandle != null)
                    {
                        this.waitForApprovalHandle.SetResult(approvalResult);
                    }
                }
            }

            [KnownType(typeof(Activities.Throw))]
            internal class ThrowException : TaskOrchestration<string, string>
            {
                public override async Task<string> RunTask(OrchestrationContext context, string message)
                {
                    if (string.IsNullOrEmpty(message))
                    {
                        // This throw happens directly in the orchestration.
                        throw new Exception(message);
                    }

                    // This throw happens in the implementation of an activity.
                    await context.ScheduleTask<string>(typeof(Activities.Throw), message);
                    return null;

                }
            }

            [KnownType(typeof(Activities.Throw))]
            internal class Throw : TaskOrchestration<string, string>
            {
                public override async Task<string> RunTask(OrchestrationContext context, string message)
                {
                    if (string.IsNullOrEmpty(message))
                    {
                        // This throw happens directly in the orchestration.
                        throw new ArgumentNullException(nameof(message));
                    }

                    // This throw happens in the implementation of an activity.
                    await context.ScheduleTask<string>(typeof(Activities.Throw), message);
                    return null;
                }
            }

            [KnownType(typeof(Activities.Throw))]
            internal class TryCatchLoop : TaskOrchestration<int, int>
            {
                public override async Task<int> RunTask(OrchestrationContext context, int iterations)
                {
                    int catchCount = 0;

                    for (int i = 0; i < iterations; i++)
                    {
                        try
                        {
                            await context.ScheduleTask<string>(typeof(Activities.Throw), "Kah-BOOOOOM!!!");
                        }
                        catch (TaskFailedException)
                        {
                            catchCount++;
                        }
                    }

                    return catchCount;
                }
            }

            [KnownType(typeof(Activities.Echo))]
            internal class Echo : TaskOrchestration<string, string>
            {
                public override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    return context.ScheduleTask<string>(typeof(Activities.Echo), input);
                }
            }

            [KnownType(typeof(Activities.EchoBytes))]
            internal class EchoBytes : TaskOrchestration<byte[], byte[]>
            {
                public override Task<byte[]> RunTask(OrchestrationContext context, byte[] input)
                {
                    return context.ScheduleTask<byte[]>(typeof(Activities.EchoBytes), input);
                }
            }

            [KnownType(typeof(Activities.WriteTableRow))]
            [KnownType(typeof(Activities.CountTableRows))]
            internal class MapReduceTableStorage : TaskOrchestration<int, int>
            {
                public override async Task<int> RunTask(OrchestrationContext context, int iterations)
                {
                    string instanceId = context.OrchestrationInstance.InstanceId;

                    var tasks = new List<Task>(iterations);
                    for (int i = 1; i <= iterations; i++)
                    {
                        tasks.Add(context.ScheduleTask<string>(
                            typeof(Activities.WriteTableRow),
                            new Tuple<string, string>(instanceId, i.ToString("000"))));
                    }

                    await Task.WhenAll(tasks);

                    return await context.ScheduleTask<int>(typeof(Activities.CountTableRows), instanceId);
                }
            }

            [KnownType(typeof(Factorial))]
            [KnownType(typeof(Activities.Multiply))]
            internal class ParentOfFactorial : TaskOrchestration<int, int>
            {
                public override Task<int> RunTask(OrchestrationContext context, int input)
                {
                    return context.CreateSubOrchestrationInstance<int>(typeof(Factorial), input);
                }
            }

            [KnownType(typeof(Activities.Hello))]
            internal class DoubleFanOut : TaskOrchestration<string, string>
            {
                public async override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    Random r = new Random();
                    var tasks = new Task<string>[5];
                    for (int i = 0; i < 5; i++)
                    {
                        int x = r.Next(10000);
                        tasks[i] = context.ScheduleTask<string>(typeof(Activities.Hello), i.ToString());
                    }

                    await Task.WhenAll(tasks);

                    var tasks2 = new Task<string>[5];
                    for (int i = 0; i < 5; i++)
                    {
                        int x = r.Next(10000);
                        tasks2[i] = context.ScheduleTask<string>(typeof(Activities.Hello), (i + 10).ToString());
                    }

                    await Task.WhenAll(tasks2);

                    return "OK";
                }
            }

            [KnownType(typeof(AutoStartOrchestration.Responder))]
            internal class AutoStartOrchestration : TaskOrchestration<string, string>
            {
                private readonly TaskCompletionSource<string> tcs
                    = new TaskCompletionSource<string>(TaskContinuationOptions.ExecuteSynchronously);

                // HACK: This is just a hack to communicate result of orchestration back to test
                public static bool OkResult;

                private static string ChannelName = Guid.NewGuid().ToString();

                public async override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    var responderId = $"@{typeof(Responder).FullName}";
                    var responderInstance = new OrchestrationInstance() { InstanceId = responderId };
                    var requestInformation = new RequestInformation { InstanceId = context.OrchestrationInstance.InstanceId, RequestId = ChannelName };

                    // send the RequestInformation containing RequestId and Instanceid of this orchestration to a not-yet-started orchestration
                    context.SendEvent(responderInstance, ChannelName, requestInformation);

                    // wait for a response event 
                    var message = await tcs.Task;
                    if (message != "hello from autostarted orchestration")
                        throw new Exception("test failed");

                    OkResult = true;

                    return "OK";
                }

                public class RequestInformation
                {
                    [JsonProperty("id")]
                    public string RequestId { get; set; }
                    public string InstanceId { get; set; }
                }

                public override void OnEvent(OrchestrationContext context, string name, string input)
                {
                    if (name == ChannelName)
                    {
                        tcs.TrySetResult(input);
                    }
                }

                public class Responder : TaskOrchestration<string, string, RequestInformation, string>
                {
                    private readonly TaskCompletionSource<string> tcs
                        = new TaskCompletionSource<string>(TaskContinuationOptions.ExecuteSynchronously);

                    public async override Task<string> RunTask(OrchestrationContext context, string input)
                    {
                        var message = await tcs.Task;
                        string responseString;

                        // send a message back to the sender
                        if (input != null)
                        {
                            responseString = "expected null input for autostarted orchestration";
                        }
                        else
                        {
                            responseString = "hello from autostarted orchestration";
                        }
                        var senderInstance = new OrchestrationInstance() { InstanceId = message };
                        context.SendEvent(senderInstance, ChannelName, responseString);

                        return "this return value is not observed by anyone";
                    }

                    public override void OnEvent(OrchestrationContext context, string name, RequestInformation input)
                    {
                        if (name == ChannelName)
                        {
                            tcs.TrySetResult(input.InstanceId);
                        }
                    }
                }
            }

            [KnownType(typeof(AbortSessionOrchestration.Activity))]
            internal class AbortSessionOrchestration : TaskOrchestration<string, string>
            {
                // This is a hacky way of keeping track of global state
                static bool abortedOrchestration = false;
                static bool abortedActivity = false;

                public async override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    if (!abortedOrchestration)
                    {
                        abortedOrchestration = true;
                        throw new SessionAbortedException();
                    }

                    try
                    {
                        await context.ScheduleTask<string>(typeof(Activity), input);
                        return (abortedOrchestration && abortedActivity).ToString();
                    }
                    catch
                    {
                        return "Test failed: The activity's SessionAbortedException should not be visible to the orchestration";
                    }
                    finally
                    {
                        // reset to ensure future executions work correctly
                        abortedOrchestration = false;
                        abortedActivity = false;
                    }
                }

                public class Activity : TaskActivity<string, string>
                {
                    protected override string Execute(TaskContext context, string input)
                    {
                        if (!abortedActivity)
                        {
                            abortedActivity = true;
                            throw new SessionAbortedException();
                        }

                        return input;
                    }
                }
            }

            internal class CurrentTimeInline : TaskOrchestration<DateTime, string>
            {
                public override Task<DateTime> RunTask(OrchestrationContext context, string input)
                {
                    return Task.FromResult(context.CurrentUtcDateTime);
                }
            }

            [KnownType(typeof(Activities.CurrentTime))]
            internal class CurrentTimeActivity : TaskOrchestration<DateTime, string>
            {
                public override Task<DateTime> RunTask(OrchestrationContext context, string input)
                {
                    return context.ScheduleTask<DateTime>(typeof(Activities.CurrentTime), input);
                }
            }

            internal class DelayedCurrentTimeInline : TaskOrchestration<DateTime, string>
            {
                public override async Task<DateTime> RunTask(OrchestrationContext context, string input)
                {
                    await context.CreateTimer<bool>(context.CurrentUtcDateTime.Add(TimeSpan.FromSeconds(3)), true);
                    return context.CurrentUtcDateTime;
                }
            }

            [KnownType(typeof(Activities.DelayedCurrentTime))]
            internal class DelayedCurrentTimeActivity : TaskOrchestration<DateTime, string>
            {
                public override Task<DateTime> RunTask(OrchestrationContext context, string input)
                {
                    return context.ScheduleTask<DateTime>(typeof(Activities.DelayedCurrentTime), input);
                }
            }
        }

        static class Entities
        {
            internal class Counter : TaskEntity<int>
            {
                public override int CreateInitialState(EntityContext<int> ctx)
                {
                    return 0;
                }

                public override ValueTask ExecuteOperationAsync(EntityContext<int> ctx)
                {
                    switch (ctx.OperationName)
                    {
                        case "get":
                            ctx.Return(ctx.State);
                            break;

                        case "increment":
                            ctx.State++;
                            ctx.Return(ctx.State);
                            break;

                        case "add":
                            ctx.State += ctx.GetInput<int>();
                            ctx.Return(ctx.State);
                            break;

                        case "set":
                            ctx.State = ctx.GetInput<int>();
                            break;

                        case "delete":
                            ctx.DeleteState();
                            break;
                    }

                    return default;
                }
            }

            //-------------- An entity that forwards a signal -----------------

            internal class Relay : TaskEntity<object>
            {
                public override ValueTask ExecuteOperationAsync(EntityContext<object> context)
                {
                    var (destination, operation) = context.GetInput<(EntityId, string)>();

                    context.SignalEntity(destination, operation);

                    return default;
                }
            }      

            // -------------- An entity that records all batch positions and batch sizes -----------------
            internal class BatchEntity : TaskEntity<List<(int,int)>>
            {
                public override List<(int, int)> CreateInitialState(EntityContext<List<(int, int)>> context)
                    => new List<(int, int)>();

                public override ValueTask ExecuteOperationAsync(EntityContext<List<(int, int)>> context)
                {
                    context.State.Add((context.BatchPosition, context.BatchSize));
                    return default;
                }         
            }

            //-------------- a very simple entity that stores a string -----------------
            // it offers two operations:
            // "set" (takes a string, assigns it to the current state, does not return anything)
            // "get" (returns a string containing the current state)// An entity that records all batch positions and batch sizes
            internal class StringStore : TaskEntity<string>
            {
                public override ValueTask ExecuteOperationAsync(EntityContext<string> context)
                {
                    switch (context.OperationName)
                    {
                        case "set":
                            context.State = context.GetInput<string>();
                            break;

                        case "get":
                            context.Return(context.State);
                            break;

                        default:
                            throw new NotImplementedException("no such operation");
                    }
                    return default;
                }
            }

            //-------------- a slightly less trivial version of the same -----------------
            // as before with two differences:
            // - "get" throws an exception if the entity does not already exist, i.e. state was not set to anything
            // - a new operation "delete" deletes the entity, i.e. clears all state
            internal class StringStore2 : TaskEntity<string>
            {
                public override ValueTask ExecuteOperationAsync(EntityContext<string> context)
                {
                    switch (context.OperationName)
                    {
                        case "delete":
                            context.DeleteState();
                            break;

                        case "set":
                            context.State = context.GetInput<string>();
                            break;

                        case "get":
                            if (!context.HasState)
                            {
                                throw new InvalidOperationException("this entity does not like 'get' when it does not have state yet");
                            }

                            context.Return(context.State);
                            break;

                        default:
                            throw new NotImplementedException("no such operation");
                    }
                    return default;
                }
            }

            //-------------- An entity that launches an orchestration -----------------
    
            internal class Launcher : TaskEntity<Launcher.State>
            {
                public class State
                {
                    public string Id;
                    public bool Done;
                }
               
                public override State CreateInitialState(EntityContext<State> context) => new State();

                public override ValueTask ExecuteOperationAsync(EntityContext<State> context)
                {
                    switch (context.OperationName)
                    {
                        case "launch":
                            {
                                context.State.Id = context.StartNewOrchestration(typeof(Orchestrations.DelayedSignal), input: context.EntityId);
                                break;
                            }

                        case "done":
                            {
                                context.State.Done = true;
                                break;
                            }

                        case "get":
                            {
                                context.Return(context.State.Done ? context.State.Id : null);
                                break;
                            }

                        default:
                            throw new NotImplementedException("no such entity operation");
                    }

                    return default;
                }
            }

            //-------------- an entity that is designed to throw certain exceptions during operations, serialization, or deserialization -----------------

            internal class FaultyEntityWithRollback : FaultyEntity
            {
                public override bool RollbackOnExceptions => true;
                public override FaultyEntity CreateInitialState(EntityContext<FaultyEntity> context) => new FaultyEntityWithRollback();
            }

            internal class FaultyEntityWithoutRollback : FaultyEntity
            {
                public override bool RollbackOnExceptions => false;
                public override FaultyEntity CreateInitialState(EntityContext<FaultyEntity> context) => new FaultyEntityWithoutRollback();
            }

            [JsonObject(MemberSerialization.OptIn)]
            internal abstract class FaultyEntity : TaskEntity<FaultyEntity>
            {
                [JsonProperty]
                public int Value { get; set; }

                [JsonProperty()]
                [JsonConverter(typeof(CustomJsonConverter))]
                public object ObjectWithFaultySerialization { get; set; }

                [JsonProperty]
                public int NumberIncrementsSent { get; set; }

                public override DataConverter ErrorDataConverter => new JsonDataConverter(new JsonSerializerSettings
                {
                    TypeNameHandling = TypeNameHandling.Objects,
                    ReferenceLoopHandling = ReferenceLoopHandling.Ignore,
                });

                public override ValueTask ExecuteOperationAsync(EntityContext<FaultyEntity> context)
                {
                    switch (context.OperationName)
                    {
                        case "exists":
                            context.Return(context.HasState);
                            break;

                        case "deletewithoutreading":
                            context.DeleteState();
                            break;

                        case "Get":
                            if (!context.HasState)
                            {
                                context.Return(0);
                            }
                            else
                            {
                                context.Return(context.State.Value);
                            }
                            break;

                        case "GetNumberIncrementsSent":
                            context.Return(context.State.NumberIncrementsSent);
                            break;

                        case "Set":
                            context.State.Value = context.GetInput<int>();
                            break;

                        case "SetToUnserializable":
                            context.State.ObjectWithFaultySerialization = new UnserializableKaboom();
                            break;

                        case "SetToUnDeserializable":
                            context.State.ObjectWithFaultySerialization = new UnDeserializableKaboom();
                            break;

                        case "SetThenThrow":
                            context.State.Value = context.GetInput<int>();
                            throw new FaultyEntity.SerializableKaboom();

                        case "Send":
                            Send();
                            break;

                        case "SendThenThrow":
                            Send();
                            throw new FaultyEntity.SerializableKaboom();

                        case "SendThenMakeUnserializable":
                            Send();
                            context.State.ObjectWithFaultySerialization = new UnserializableKaboom();
                            break;

                        case "Delete":
                            context.DeleteState();
                            break;

                        case "DeleteThenThrow":
                            context.DeleteState();
                            throw new FaultyEntity.SerializableKaboom();

                        case "Throw":
                            throw new FaultyEntity.SerializableKaboom();

                        case "ThrowUnserializable":
                            throw new FaultyEntity.UnserializableKaboom();

                        case "ThrowUnDeserializable":
                            throw new FaultyEntity.UnDeserializableKaboom();
                    }
                    return default;

                    void Send()
                    {
                        var desc = $"{++context.State.NumberIncrementsSent}:{context.State.Value}";
                        context.SignalEntity(context.GetInput<EntityId>(), desc);
                    }
                }

                public class CustomJsonConverter : JsonConverter
                {
                    public override bool CanConvert(Type objectType)
                    {
                        return objectType == typeof(SerializableKaboom)
                            || objectType == typeof(UnserializableKaboom)
                            || objectType == typeof(UnDeserializableKaboom);
                    }

                    public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
                    {
                        if (reader.TokenType == JsonToken.Null)
                        {
                            return null;
                        }

                        var typename = serializer.Deserialize<string>(reader);

                        if (typename != nameof(SerializableKaboom))
                        {
                            throw new JsonSerializationException("purposefully designed to not be deserializable");
                        }

                        return new SerializableKaboom();
                    }

                    public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
                    {
                        if (value is UnserializableKaboom)
                        {
                            throw new JsonSerializationException("purposefully designed to not be serializable");
                        }

                        serializer.Serialize(writer, value.GetType().Name);
                    }
                }

                public class UnserializableKaboom : Exception
                {
                }

                public class SerializableKaboom : Exception
                {
                }

                public class UnDeserializableKaboom : Exception
                {
                }
            }

        }


        static class Activities
        {
            internal class HelloFailActivity : TaskActivity<string, string>
            {
                public static bool ShouldFail = true;
                protected override string Execute(TaskContext context, string input)
                {
                    if (string.IsNullOrEmpty(input))
                    {
                        throw new ArgumentNullException(nameof(input));
                    }

                    if (ShouldFail)
                    {
                        throw new Exception("Simulating unhandled activty function failure...");
                    }

                    return $"Hello, {input}!";
                }
            }

            internal class HelloFailFanOut : TaskActivity<string, string>
            {
                public static bool ShouldFail1 = true;
                public static bool ShouldFail2 = true;
                protected override string Execute(TaskContext context, string input)
                {
                    if (string.IsNullOrEmpty(input))
                    {
                        throw new ArgumentNullException(nameof(input));
                    }

                    if (ShouldFail1 || ShouldFail2) //&& (input == "0" || input == "2"))
                    {
                        throw new Exception("Simulating unhandled activty function failure...");
                    }

                    return $"Hello, {input}!";
                }
            }

            internal class HelloFailMultipleActivity : TaskActivity<string, string>
            {
                public static bool ShouldFail1 = true;
                public static bool ShouldFail2 = true;
                protected override string Execute(TaskContext context, string input)
                {
                    if (string.IsNullOrEmpty(input))
                    {
                        throw new ArgumentNullException(nameof(input));
                    }

                    if (ShouldFail1 || ShouldFail2)
                    {
                        throw new Exception("Simulating unhandled activty function failure...");
                    }

                    return $"Hello, {input}!";
                }
            }

            internal class HelloFailNestedSuborchestration : TaskActivity<string, string>
            {
                public static bool ShouldFail1 = true;
                public static bool ShouldFail2 = true;
                protected override string Execute(TaskContext context, string input)
                {
                    if (string.IsNullOrEmpty(input))
                    {
                        throw new ArgumentNullException(nameof(input));
                    }

                    if (ShouldFail1 || ShouldFail2)
                    {
                        throw new Exception("Simulating unhandled activty function failure...");
                    }

                    return $"Hello, {input}!";
                }
            }

            internal class HelloFailSubOrchestrationActivity : TaskActivity<string, string>
            {
                public static bool ShouldFail1 = true;
                public static bool ShouldFail2 = true;
                protected override string Execute(TaskContext context, string input)
                {
                    if (string.IsNullOrEmpty(input))
                    {
                        throw new ArgumentNullException(nameof(input));
                    }

                    if (ShouldFail1 || ShouldFail2)
                    {
                        throw new Exception("Simulating unhandled activty function failure...");
                    }

                    return $"Hello, {input}!";
                }
            }

            internal class Hello : TaskActivity<string, string>
            {
                protected override string Execute(TaskContext context, string input)
                {
                    if (string.IsNullOrEmpty(input))
                    {
                        throw new ArgumentNullException(nameof(input));
                    }
                    return $"Hello, {input}!";
                }
            }

            internal class Multiply : TaskActivity<long[], long>
            {
                protected override long Execute(TaskContext context, long[] values)
                {
                    return values[0] * values[1];
                }
            }

            internal class MultiplyMultipleActivityFail : TaskActivity<long[], long>
            {
                public static bool ShouldFail1 = true;
                public static bool ShouldFail2 = true;
                protected override long Execute(TaskContext context, long[] values)
                {
                    if ((ShouldFail1 && (values[1] == 1)) || (ShouldFail2 && values[1] == 2))
                    {
                        throw new Exception("Simulating a transient, unhandled exception");
                    }

                    return values[0] * values[1];
                }
            }
            internal class MultiplyFailOrchestration : TaskActivity<long[], long>
            {
                public static bool ShouldFail1 = true;
                public static bool ShouldFail2 = true;
                protected override long Execute(TaskContext context, long[] values)
                {
                    if ((ShouldFail1 && (values[1] == 1)) || (ShouldFail2 && values[1] == 2))
                    {
                        throw new Exception("Simulating a transient, unhandled exception");
                    }

                    return values[0] * values[1];
                }
            }


            internal class GetFileList : TaskActivity<string, string[]>
            {
                protected override string[] Execute(TaskContext context, string directory)
                {
                    return Directory.GetFiles(directory, "*", SearchOption.TopDirectoryOnly);
                }
            }

            internal class GetFileSize : TaskActivity<string, long>
            {
                protected override long Execute(TaskContext context, string fileName)
                {
                    var info = new FileInfo(fileName);
                    return info.Length;
                }
            }

            internal class Throw : TaskActivity<string, string>
            {
                protected override string Execute(TaskContext context, string message)
                {
                    throw new Exception(message);
                }
            }

            internal class WriteTableRow : TaskActivity<Tuple<string, string>, string>
            {
                static CloudTable cachedTable;

                internal static CloudTable TestCloudTable
                {
                    get
                    {
                        if (cachedTable == null)
                        {
                            string connectionString = TestHelpers.GetTestStorageAccountConnectionString();
                            CloudTable table = CloudStorageAccount.Parse(connectionString).CreateCloudTableClient().GetTableReference("TestTable");
                            table.CreateIfNotExistsAsync().Wait();
                            cachedTable = table;
                        }

                        return cachedTable;
                    }
                }

                protected override string Execute(TaskContext context, Tuple<string, string> rowData)
                {
                    var entity = new DynamicTableEntity(
                        partitionKey: rowData.Item1,
                        rowKey: $"{rowData.Item2}.{Guid.NewGuid():N}");
                    TestCloudTable.ExecuteAsync(TableOperation.Insert(entity)).Wait();
                    return null;
                }
            }

            internal class CountTableRows : TaskActivity<string, int>
            {
                protected override int Execute(TaskContext context, string partitionKey)
                {
                    var query = new TableQuery<DynamicTableEntity>().Where(
                        TableQuery.GenerateFilterCondition(
                            "PartitionKey",
                            QueryComparisons.Equal,
                            partitionKey));

                    return WriteTableRow.TestCloudTable.ExecuteQuerySegmentedAsync(query, new TableContinuationToken()).Result.Count();
                }
            }
            internal class Echo : TaskActivity<string, string>
            {
                protected override string Execute(TaskContext context, string input)
                {
                    return input;
                }
            }

            internal class EchoBytes : TaskActivity<byte[], byte[]>
            {
                protected override byte[] Execute(TaskContext context, byte[] input)
                {
                    return input;
                }
            }

            internal class CurrentTime : TaskActivity<string, DateTime>
            {
                protected override DateTime Execute(TaskContext context, string input)
                {
                    if (string.IsNullOrEmpty(input))
                    {
                        throw new ArgumentNullException(nameof(input));
                    }
                    return DateTime.UtcNow;
                }
            }

            internal class DelayedCurrentTime : TaskActivity<string, DateTime>
            {
                protected override DateTime Execute(TaskContext context, string input)
                {
                    if (string.IsNullOrEmpty(input))
                    {
                        throw new ArgumentNullException(nameof(input));
                    }

                    Thread.Sleep(TimeSpan.FromSeconds(3));

                    return DateTime.UtcNow;
                }
            }
        }
    }
}
