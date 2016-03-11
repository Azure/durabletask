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

namespace DurableTask.Test
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Common;
    using DurableTask.Exceptions;
    using DurableTask.Serializing;

    internal class FakeTaskActivityExecutor
    {
        readonly JsonDataConverter dataConverter;
        readonly NameVersionObjectManager<TaskActivity> objectManager;
        int pendingExecutions;

        public FakeTaskActivityExecutor(NameVersionObjectManager<TaskActivity> objectManager)
        {
            dataConverter = new JsonDataConverter();
            pendingExecutions = 0;
            this.objectManager = objectManager;
        }

        public bool HasPendingExecutions
        {
            get { return pendingExecutions > 0; }
        }

        public async Task<TResult> ExecuteTask<TResult>(string name, string version, params object[] parameters)
        {
            string serializedInput = dataConverter.Serialize(parameters);
            TaskActivity activity = objectManager.GetObject(name, version);

            Interlocked.Increment(ref pendingExecutions);

            string serializedResult = await Task.Factory.StartNew(() =>
            {
                try
                {
                    string result = activity.RunAsync(null, serializedInput).Result;
                    return result;
                }
                catch (AggregateException e)
                {
                    e = e.Flatten();
                    if (e.InnerException is TaskFailureException)
                    {
                        var taskFailureException = e.InnerException as TaskFailureException;
                        Exception cause = Utils.RetrieveCause(taskFailureException.Details, dataConverter);
                        throw new TaskFailedException(0, 0, name, version, taskFailureException.Message, cause);
                    }
                    throw new TaskFailedException(0, 0, name, version, e.Message, e);
                }
                finally
                {
                    Interlocked.Decrement(ref pendingExecutions);
                }
            });

            var r = dataConverter.Deserialize<TResult>(serializedResult);
            return r;
        }
    }
}