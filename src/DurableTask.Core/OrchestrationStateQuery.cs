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
    using System.Linq;

    /// <summary>
    ///     Query class that can be used to filter results from the Orchestration instance store.
    ///     Instance methods are not thread safe.
    /// </summary>
    public class OrchestrationStateQuery
    {
        // if we get multiple filters in a state query, we will pick one as the primary filter to pass on in the 
        // azure table store query. the remaining filters will be used to trim the results in-memory
        // this table gives the precedence of the filters. higher number will be selected over the lower one.
        //
        static readonly Dictionary<Type, int> FilterPrecedenceMap = new Dictionary<Type, int>
        {
            {typeof (OrchestrationStateTimeRangeFilter), 10},
            {typeof (OrchestrationStateInstanceFilter), 5},
            {typeof (OrchestrationStateNameVersionFilter), 3},
            {typeof (OrchestrationStateStatusFilter), 1},
        };

        /// <summary>
        ///     Query class that can be used to filter results from the Orchestration instance store.
        ///     Instance methods are not thread safe.
        /// </summary>
        public OrchestrationStateQuery()
        {
            FilterMap = new Dictionary<Type, OrchestrationStateQueryFilter>();
        }

        /// <summary>
        /// Gets the FilterMap for the query
        /// </summary>
        public IDictionary<Type, OrchestrationStateQueryFilter> FilterMap { get; private set; }

        /// <summary>
        /// Gets the primary_filter, collection_of(secondary_filters) for the query
        /// </summary>
        public Tuple<OrchestrationStateQueryFilter, IEnumerable<OrchestrationStateQueryFilter>> GetFilters()
        {
            ICollection<OrchestrationStateQueryFilter> filters = FilterMap.Values;
            if (filters.Count == 0)
            {
                return null;
            }

            var secondaryFilters = new List<OrchestrationStateQueryFilter>();

            OrchestrationStateQueryFilter primaryFilter = filters.First();
            int primaryFilterPrecedence = SafeGetFilterPrecedence(primaryFilter);

            if (filters.Count > 1)
            {
                foreach (OrchestrationStateQueryFilter filter in filters)
                {
                    int newPrecedence = SafeGetFilterPrecedence(filter);
                    if (newPrecedence > primaryFilterPrecedence)
                    {
                        secondaryFilters.Add(primaryFilter);

                        primaryFilter = filter;
                        primaryFilterPrecedence = newPrecedence;
                    }
                    else
                    {
                        secondaryFilters.Add(filter);
                    }
                }
            }
            return new Tuple<OrchestrationStateQueryFilter, IEnumerable<OrchestrationStateQueryFilter>>(
                primaryFilter, secondaryFilters);
        }

        int SafeGetFilterPrecedence(OrchestrationStateQueryFilter filter)
        {
            if (!FilterPrecedenceMap.ContainsKey(filter.GetType()))
            {
                throw new InvalidOperationException("Unknown filter type: " + filter.GetType());
            }

            return FilterPrecedenceMap[filter.GetType()];
        }


        /// <summary>
        ///     Adds an exact match instance id filter on the returned orchestrations
        /// </summary>
        /// <param name="instanceId">Instance Id to filter by</param>
        /// <returns></returns>
        public OrchestrationStateQuery AddInstanceFilter(string instanceId)
        {
            return AddInstanceFilter(instanceId, false);
        }

        /// <summary>
        ///     Adds an exact match instance id filter on the returned orchestrations
        /// </summary>
        /// <param name="instanceId">Instance Id to filter by</param>
        /// <param name="executionId">Execution Id to filter by</param>
        /// <returns></returns>
        public OrchestrationStateQuery AddInstanceFilter(string instanceId, string executionId)
        {
            if (FilterMap.ContainsKey(typeof (OrchestrationStateInstanceFilter)))
            {
                throw new ArgumentException("Cannot add more than one instance filters");
            }

            FilterMap.Add(typeof (OrchestrationStateInstanceFilter),
                new OrchestrationStateInstanceFilter
                {
                    InstanceId = instanceId,
                    ExecutionId = executionId,
                    StartsWith = false
                });

            return this;
        }

        /// <summary>
        ///     Adds an instance id filter on the returned orchestrations
        /// </summary>
        /// <param name="instanceId">Instance Id to filter by</param>
        /// <param name="startsWith">Exact match if set to false, otherwise do a starts-with match</param>
        /// <returns></returns>
        public OrchestrationStateQuery AddInstanceFilter(string instanceId, bool startsWith)
        {
            if (FilterMap.ContainsKey(typeof (OrchestrationStateInstanceFilter)))
            {
                throw new ArgumentException("Cannot add more than one instance filters");
            }

            FilterMap.Add(typeof (OrchestrationStateInstanceFilter),
                new OrchestrationStateInstanceFilter {InstanceId = instanceId, StartsWith = startsWith});

            return this;
        }

        /// <summary>
        ///     Adds a name filter on the returned orchestrations
        /// </summary>
        /// <param name="name">The name of the orchestration to filter by</param>
        /// <returns></returns>
        public OrchestrationStateQuery AddNameVersionFilter(string name)
        {
            return AddNameVersionFilter(name, null);
        }

        /// <summary>
        ///     Adds a name/version filter on the returned orchestations
        /// </summary>
        /// <param name="name">The name of the orchestration to filter by</param>
        /// <param name="version">The version of the orchestration to filter by</param>
        /// <returns></returns>
        public OrchestrationStateQuery AddNameVersionFilter(string name, string version)
        {
            if (FilterMap.ContainsKey(typeof (OrchestrationStateNameVersionFilter)))
            {
                throw new ArgumentException("Cannot add more than one name/version filters");
            }

            FilterMap.Add(typeof (OrchestrationStateNameVersionFilter),
                new OrchestrationStateNameVersionFilter {Name = name, Version = version});

            return this;
        }

        /// <summary>
        ///     Adds a status filter on the returned orchestrations
        /// </summary>
        /// <param name="status">The status to filter by</param>
        /// <returns></returns>
        public OrchestrationStateQuery AddStatusFilter(OrchestrationStatus status)
        {
            if (FilterMap.ContainsKey(typeof (OrchestrationStateStatusFilter)))
            {
                throw new ArgumentException("Cannot add more than one status filters");
            }

            FilterMap.Add(typeof (OrchestrationStateStatusFilter),
                new OrchestrationStateStatusFilter {Status = status});

            return this;
        }

        /// <summary>
        ///     Adds a time range filter on the returned orchestrations
        /// </summary>
        /// <param name="startTime">Start of the time range to filter by</param>
        /// <param name="endTime">End of the time range to filter by</param>
        /// <param name="filterType">Type of orchestration timestamp to apply filter on</param>
        /// <returns></returns>
        public OrchestrationStateQuery AddTimeRangeFilter(DateTime startTime, DateTime endTime,
            OrchestrationStateTimeRangeFilterType filterType)
        {
            if (FilterMap.ContainsKey(typeof (OrchestrationStateTimeRangeFilter)))
            {
                throw new ArgumentException("Cannot add more than one time range filters");
            }

            FilterMap.Add(typeof (OrchestrationStateTimeRangeFilter),
                new OrchestrationStateTimeRangeFilter
                {
                    StartTime = startTime,
                    EndTime = endTime,
                    FilterType = filterType
                });

            return this;
        }
    }
}