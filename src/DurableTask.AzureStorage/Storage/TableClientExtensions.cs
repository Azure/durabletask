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
namespace DurableTask.AzureStorage.Storage
{
    using System;
    using System.Linq.Expressions;
    using System.Reflection;
    using Azure.Data.Tables;

    static class TableClientExtensions
    {
        static readonly Func<TableClient, Uri?> getEndpointFunc = CreateGetEndpointFunc();

        public static Uri? GetEndpoint(this TableClient client)
        {
            return getEndpointFunc(client);
        }

        static Func<TableClient, Uri?> CreateGetEndpointFunc()
        {
            // The Endpoint isn't publicly available, so we'll attempt to find it via reflection.
            // If we cannot find it due to changes in the underlying binary, we'll simply return null
            FieldInfo? endpointField = typeof(TableClient).GetField("_endpoint", BindingFlags.Instance | BindingFlags.NonPublic);

            ParameterExpression clientParameter = Expression.Parameter(typeof(TableClient), "client");
            Expression body = endpointField == null ? Expression.Constant(null) : Expression.Field(clientParameter, endpointField);
            return (Func<TableClient, Uri?>)Expression.Lambda(typeof(Func<TableClient, Uri>), body, clientParameter).Compile();
        }
    }
}
