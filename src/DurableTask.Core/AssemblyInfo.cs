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

using System.Runtime.CompilerServices;

#if SIGN_ASSEMBLY
// Grants the signed Azure Functions Durable Task host (in-process worker) access to internal
// members, such as the worker-version exclusion used for its own infrastructure orchestrations.
// A strongly-named assembly must specify the full public key, not just the public key token, of
// any strongly-named friend assembly.
[assembly: InternalsVisibleTo("Microsoft.Azure.WebJobs.Extensions.DurableTask, PublicKey=0024000004800000940000000602000000240000525341310004000001000100cd1dabd5a893b40e75dc901fe7293db4a3caf9cd4d3e3ed6178d49cd476969abe74a9e0b7f4a0bb15edca48758155d35a4f05e6e852fff1b319d103b39ba04acbadd278c2753627c95e1f6f6582425374b92f51cca3deb0d2aab9de3ecda7753900a31f70a236f163006beefffe282888f85e3c76d1205ec7dfef7fa472a17b1")]
#else
[assembly: InternalsVisibleTo("DurableTask.Core.Tests")]
[assembly: InternalsVisibleTo("DurableTask.Framework.Tests")]
[assembly: InternalsVisibleTo("DurableTask.ServiceBus.Tests")]
// An unsigned assembly does not enforce a public key on its friends, so the same host assembly
// can be declared by simple name alone for unsigned (non-Release) builds of this assembly.
[assembly: InternalsVisibleTo("Microsoft.Azure.WebJobs.Extensions.DurableTask")]
#endif
