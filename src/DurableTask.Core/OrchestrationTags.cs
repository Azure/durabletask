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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DurableTask.Core
{
    /// <summary>
    /// Common code for dealing with orchestration tags. Orchestration tags are string-typed 
    /// properties that can be explicitly assigned when orchestrations or suborchestrations
    /// are created. A suborchestration automatically inherits the tags of its parent orchestration.
    /// </summary>
    public static class OrchestrationTags
    {
        /// <summary>
        /// A special orchestration tag used for indicating that a suborchestration is fire-and-forget,
        /// i.e. the parent orchestration does not wait for the result.
        /// </summary>
        /// <remarks>
        /// Tags are generally intended for application-specific purposes and ignored by the runtime,
        /// except for this special tag. Unlike general application-defined tags, this tag is not 
        /// automatically inherited by sub-orchestrations.
        /// </remarks>
        public const string FireAndForget = "FireAndForget";

        /// <summary>
        /// Check whether the given tags contain the fire and forget tag
        /// </summary>
        /// <param name="tags"></param>
        /// <returns></returns>
        internal static bool IsTaggedAsFireAndForget(IDictionary<string, string> tags)
        {
            return tags != null && tags.ContainsKey(FireAndForget);
        }

        internal static IDictionary<string, string> MergeTags(
            IDictionary<string, string> newTags,
            IDictionary<string, string> existingTags)
        {
            if (existingTags == null)
            {
                return newTags;
            }
            else if (newTags != null)
            {
                // We merge the two dictionaries of tags, with new tags overriding existing tags
                return newTags.Concat(
                    existingTags.Where(k => !newTags.ContainsKey(k.Key) && k.Key != FireAndForget))
                    .ToDictionary(x => x.Key, y => y.Value);
            }
            else if (!existingTags.ContainsKey(FireAndForget))
            {
                return existingTags;
            }
            else
            {
                return existingTags
                    .Where(k => k.Key != FireAndForget)
                    .ToDictionary(x => x.Key, y => y.Value);
            }
        }
    }
}
