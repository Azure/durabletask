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
    using System.Collections.Specialized;
    using System.IO;
    using System.Linq;
    using System.Xml;

    class ApplicationInfoReader
    {
        string applicationRootPath;
        string applicationPackagePath;

        readonly XmlElement applicationManifestRoot;
        readonly XmlNamespaceManager applicationManifestNamespaceManager;
        readonly XmlElement serviceManifestRoot;
        readonly XmlNamespaceManager serviceManifestNamespaceManager;

        const string ApplicationManifestNodePath = "/sf:ApplicationManifest";
        const string ApplicationNameAttribute = "ApplicationTypeName";
        const string ApplicationVersionAttribute = "ApplicationTypeVersion";

        const string StatefulServiceNodePath = "/sf:ServiceManifest/sf:ServiceTypes/sf:StatefulServiceType";
        const string StatelessServiceNodePath = "/sf:ServiceManifest/sf:ServiceTypes/sf:StatelessServiceType";
        const string ServiceTypeNameAttribute = "ServiceTypeName";

        public ApplicationInfoReader(string applicationRootPath)
        {
            this.applicationRootPath = applicationRootPath;
            this.applicationPackagePath = Path.Combine(applicationRootPath, @"pkg\Debug");

            var applicationManifest = new XmlDocument();
            applicationManifest.Load(Path.Combine(this.applicationPackagePath, "ApplicationManifest.xml"));
            this.applicationManifestNamespaceManager = GetXmlNamespaceManager(applicationManifest.NameTable);
            this.applicationManifestRoot = applicationManifest.DocumentElement;

            var serviceManifest = new XmlDocument();
            var serviceDirectory = Directory.EnumerateDirectories(this.applicationPackagePath).First();
            serviceManifest.Load(Path.Combine(Path.Combine(this.applicationPackagePath, serviceDirectory), "ServiceManifest.xml"));
            this.serviceManifestNamespaceManager = GetXmlNamespaceManager(serviceManifest.NameTable);
            this.serviceManifestRoot = serviceManifest.DocumentElement;
        }

        public string GetApplicationName()
        {
            return GetSingleNodeAttributeValue(applicationManifestRoot, applicationManifestNamespaceManager, ApplicationManifestNodePath, ApplicationNameAttribute);
        }

        public string GetApplicationVersion()
        {
            return GetSingleNodeAttributeValue(applicationManifestRoot, applicationManifestNamespaceManager, ApplicationManifestNodePath, ApplicationVersionAttribute);
        }

        public string GetServiceName()
        {
            return GetSingleNodeAttributeValue(serviceManifestRoot, serviceManifestNamespaceManager, StatefulServiceNodePath, ServiceTypeNameAttribute) ??
                   GetSingleNodeAttributeValue(serviceManifestRoot, serviceManifestNamespaceManager, StatelessServiceNodePath, ServiceTypeNameAttribute);
        }

        public NameValueCollection GetApplicationParameters()
        {
            var applicationParametersPath = Path.Combine(applicationRootPath, @"ApplicationParameters\Local.5Node.xml");

            var applicationParameters = new XmlDocument();
            applicationParameters.Load(applicationParametersPath);

            var parametersPath = "/sf:Application/sf:Parameters/sf:Parameter";
            var parameters = new NameValueCollection();

            foreach (var parameterNode in applicationParameters.DocumentElement.SelectNodes(parametersPath, GetXmlNamespaceManager(applicationParameters.NameTable)).OfType<XmlNode>())
            {
                parameters.Add(parameterNode.Attributes["Name"].Value, parameterNode.Attributes["Value"].Value);
            }

            return parameters;
        }

        public string ApplicationPackagePath => this.applicationPackagePath;

        string GetSingleNodeAttributeValue(XmlElement root, XmlNamespaceManager namespaceManager, string queryPath, string attributeName)
        {
            var node = root.SelectSingleNode(queryPath, namespaceManager);
            return node?.Attributes?[attributeName].Value;
        }

        XmlNamespaceManager GetXmlNamespaceManager(XmlNameTable nameTable)
        {
            var result = new XmlNamespaceManager(nameTable);
            result.AddNamespace("sf", "http://schemas.microsoft.com/2011/01/fabric");
            return result;
        }
    }
}
