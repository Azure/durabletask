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

namespace DurableTask.Core
{
    using System;
    using System.Collections.Generic;

    internal class NameVersionObjectManager<T> : INameVersionObjectManager<T>
    {
        readonly IDictionary<string, ObjectCreator<T>> creators;
        readonly object thisLock = new object();

        public NameVersionObjectManager()
        {
            creators = new Dictionary<string, ObjectCreator<T>>();
        }

        public void Add(ObjectCreator<T> creator)
        {
            lock (thisLock)
            {
                string key = GetKey(creator.Name, creator.Version);

                if (creators.ContainsKey(key))
                {
                    throw new InvalidOperationException("Duplicate entry detected: " + creator.Name + " " +
                                                        creator.Version);
                }

                creators.Add(key, creator);
            }
        }

        public T GetObject(string name, string version)
        {
            string key = GetKey(name, version);

            lock (thisLock)
            {
                ObjectCreator<T> creator = null;
                if (creators.TryGetValue(key, out creator))
                {
                    return creator.Create();
                }

                return default(T);
            }
        }

        string GetKey(string name, string version)
        {
            return name + "_" + version;
        }
    }
}