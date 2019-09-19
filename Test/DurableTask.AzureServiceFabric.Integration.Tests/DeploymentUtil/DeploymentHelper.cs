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

namespace DurableTask.AzureServiceFabric.Integration.Tests.DeploymentUtil
{
    using System;
    using System.Fabric;
    using System.Fabric.Description;
    using System.Fabric.Health;
    using System.Fabric.Query;
    using System.Linq;
    using System.Threading.Tasks;
    using System.Xml;

    static class DeploymentHelper
    {
        static readonly FabricClient client = new FabricClient();

        public static async Task DeployAsync(string applicationRootPath)
        {
            var appInfoReader = new ApplicationInfoReader(applicationRootPath);
            var serviceName = new Uri(Constants.TestFabricApplicationAddress);
            var applicationDescription =
                new ApplicationDescription(
                    new Uri($"fabric:/{appInfoReader.GetApplicationName()}"),
                    appInfoReader.GetApplicationName(),
                    appInfoReader.GetApplicationVersion(),
                    appInfoReader.GetApplicationParameters());

            await DeployAsync(appInfoReader.ApplicationPackagePath, applicationDescription);
            await WaitForHealthStatusAsync(applicationDescription, serviceName);
        }

        public static async Task CleanAsync()
        {
            var applications = await client.QueryManager.GetApplicationListAsync();
            foreach (var application in applications)
            {
                await client.ApplicationManager.DeleteApplicationAsync(new DeleteApplicationDescription(application.ApplicationName) { ForceDelete = true });

                foreach (var node in await client.QueryManager.GetNodeListAsync())
                {
                    var replicas = (await client.QueryManager.GetDeployedReplicaListAsync(node.NodeName, application.ApplicationName)).OfType<DeployedStatefulServiceReplica>();
                    foreach (var replica in replicas)
                    {
                        await client.ServiceManager.RemoveReplicaAsync(node.NodeName, replica.Partitionid, replica.ReplicaId);
                    }
                }
            }

            var applicationTypeList = await client.QueryManager.GetApplicationTypeListAsync();
            foreach (var applicationType in applicationTypeList)
            {
                await client.ApplicationManager.UnprovisionApplicationAsync(applicationType.ApplicationTypeName, applicationType.ApplicationTypeVersion);
            }
        }

        static async Task DeployAsync(string applicationPackagePath, ApplicationDescription applicationDescription)
        {
            var clusterManifest = new XmlDocument();
            var clusterManifestXml = await client.ClusterManager.GetClusterManifestAsync();
            clusterManifest.LoadXml(clusterManifestXml);
            var namespaceManager = new XmlNamespaceManager(clusterManifest.NameTable);
            namespaceManager.AddNamespace("sf", "http://schemas.microsoft.com/2011/01/fabric");
            var imageStoreConnectionStringPath = "/sf:ClusterManifest/sf:FabricSettings/sf:Section[@Name='Management']/sf:Parameter[@Name='ImageStoreConnectionString']";
            var imageStoreConnectionStringNode = clusterManifest.DocumentElement.SelectSingleNode(imageStoreConnectionStringPath, namespaceManager);
            var imageStoreConnectionString = imageStoreConnectionStringNode.Attributes["Value"].Value;

            var path = Guid.NewGuid().ToString();
            client.ApplicationManager.CopyApplicationPackage(imageStoreConnectionString, applicationPackagePath, path);

            await client.ApplicationManager.ProvisionApplicationAsync(path);

            client.ApplicationManager.RemoveApplicationPackage(imageStoreConnectionString, path);

            await client.ApplicationManager.CreateApplicationAsync(applicationDescription);
        }

        static async Task WaitForHealthStatusAsync(ApplicationDescription applicationDescription, Uri serviceName)
        {
            while (true)
            {
                await Task.Delay(TimeSpan.FromSeconds(2));
                var states = (await client.HealthManager.GetApplicationHealthAsync(applicationDescription.ApplicationName)).ServiceHealthStates;
                if (states.Any() && states.All(s => s.AggregatedHealthState == HealthState.Ok))
                {
                    return;
                }
                if (states.Any() && states.Any(s => s.AggregatedHealthState == HealthState.Error || s.AggregatedHealthState == HealthState.Invalid))
                {
                    throw new Exception($"Deployment of {serviceName} failed");
                }
            }
        }
    }
}
