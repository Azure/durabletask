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
namespace DurableTask.AzureStorage.Net
{
    using System;

    static class UriPath
    {
        public static string Combine(string path1, string path2)
        {
            if (path1 == null)
            {
                throw new ArgumentNullException(nameof(path1));
            }

            if (path2 == null)
            {
                throw new ArgumentNullException(nameof(path2));
            }

            if (path1.Length == 0)
            {
                return path2;
            }
            else if (path2.Length == 0)
            {
                return path1;
            }

            // Path1 ends with a '/'
            // E.g. path1 = "foo/"
            if (path1[path1.Length - 1] == '/')
            {
                return path2[0] == '/'
                    ? path1 + path2.Substring(1) // E.g. Combine("foo/", "/bar") == "foo/bar"
                    : path1 + path2;             // E.g. Combine("foo/", "bar") == "foo/bar"
            }
            else if (path2[0] == '/')
            {
                // E.g. Combine("foo", "/bar") == "foo/bar"
                return path1 + path2;
            }
            else
            {
                // E.g. Combine("foo", "bar") == "foo/bar"
                return path1 + '/' + path2;
            }
        }
    }
}
