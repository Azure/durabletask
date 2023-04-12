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
        public static Uri? GetUri(this TableClient tableClient)
        {
            if (tableClient == null)
            {
                throw new ArgumentNullException(nameof(tableClient));
            }

            Uri? endpoint = GetEndpointFunc(tableClient);
            return endpoint != null ? new TableUriBuilder(endpoint) { Tablename = tableClient.Name }.ToUri() : null;
        }

        static readonly Func<TableClient, Uri?> GetEndpointFunc = CreateGetEndpointFunc();

        static Func<TableClient, Uri?> CreateGetEndpointFunc()
        {
            Type tableClientType = typeof(TableClient);
            ParameterExpression clientParam = Expression.Parameter(typeof(TableClient), "client");
            FieldInfo? endpointField = tableClientType.GetField("_endpoint", BindingFlags.Instance | BindingFlags.NonPublic);

            Expression<Func<TableClient, Uri?>> lambdaExpr = endpointField != null
                ? Expression.Lambda<Func<TableClient, Uri?>>(Expression.Field(clientParam, endpointField), clientParam)
                : Expression.Lambda<Func<TableClient, Uri?>>(Expression.Constant(null, typeof(Uri)), clientParam);

            return lambdaExpr.Compile();
        }
    }
}
