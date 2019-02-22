﻿//  ----------------------------------------------------------------------------------
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

namespace DurableTask.ServiceFabric.Service
{
    using System;
    using System.Collections.Generic;

    using DurableTask.Core;
    using Microsoft.ServiceFabric.Services.Communication.Runtime;


    /// <summary>
    /// Fabric Service settings.
    /// </summary>
    public interface IFabricServiceSettings
    {
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        IEnumerable<Type> GetActivityTypes();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        IEnumerable<KeyValuePair<string, TaskOrchestration>> GetTaskOrchestrations();


        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        IEnumerable<Type> GetOrchestrationTypes();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        int GetActivityDispatcherCount();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        int GetOrchestrationDispatcherCount();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        IEnumerable<ServiceReplicaListener> GetServiceReplicaListeners();
    }
}
