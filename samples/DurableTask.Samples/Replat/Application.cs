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

namespace DurableTask.Samples.Replat
{
    public class Application
    {
        /// <summary>
        /// The name of the application in the management database
        /// </summary>
        public string Name { get; set; }

        public string Region { get; set; }

        /// <summary>
        /// The name of the site in Antares. Typically this is the same as <see cref="Name"/> but can be different in DR
        /// scenarios.
        /// </summary>
        public string SiteName { get; set; }

        /// <summary>
        /// The runtime platform of the application
        /// </summary>
        public RuntimePlatform Platform { get; set; }
    }

    public enum RuntimePlatform
    {
        /// <summary>
        /// Node.js platform. This is the default.
        /// </summary>
        Node = 0,

        /// <summary>
        /// .NET platform.
        /// </summary>
        DotNet = 1
    }
}
