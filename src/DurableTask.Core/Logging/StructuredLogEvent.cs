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

namespace DurableTask.Core.Logging
{
    using System;
    using System.Collections;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Abstract base class for all structured log events. This base class implements
    /// <see cref="IReadOnlyDictionary{String, Object}"/>using reflection to make it easy
    /// to support structured log events for providers like Application Insights.
    /// </summary>
    public abstract class StructuredLogEvent : ILogEvent, IReadOnlyDictionary<string, object>
    {
        // We reflect over all the properties just once and reuse the cached property set for subsequent log
        // statements to minimize the overhead of using reflection.
        static readonly ConcurrentDictionary<Type, IReadOnlyDictionary<string, PropertyInfo>> SharedPropertiesCache =
            new ConcurrentDictionary<Type, IReadOnlyDictionary<string, PropertyInfo>>();

        IReadOnlyDictionary<string, PropertyInfo> Properties => GetProperties(this.GetType());

        string logMessage;

        /// <inheritdoc />
        public abstract EventId EventId { get; }

        /// <inheritdoc />
        public abstract LogLevel Level { get; }

        string ILogEvent.FormattedMessage
        {
            get
            {
                // We assume all log events are immutable, which means we can cache the generated
                // log message. This is useful in cases where there are multiple ILoggers in the pipeline
                // because we can avoid generating the formatted message multiple times for the same event.
                if (this.logMessage == null)
                {
                    this.logMessage = this.CreateLogMessage();
                }

                return this.logMessage;
            }
        }

        /// <summary>
        /// When implemented by a derived class, generates a formatted log message.
        /// </summary>
        protected abstract string CreateLogMessage();

        IEnumerable<string> IReadOnlyDictionary<string, object>.Keys => this.Properties.Keys;

        IEnumerable<object> IReadOnlyDictionary<string, object>.Values => this.Properties.Values.Select(p => p.GetValue(this));

        int IReadOnlyCollection<KeyValuePair<string, object>>.Count => this.Properties.Count;

        object IReadOnlyDictionary<string, object>.this[string key] => this.Properties[key].GetValue(this);

        bool IReadOnlyDictionary<string, object>.ContainsKey(string key) => this.Properties.ContainsKey(key);

        bool IReadOnlyDictionary<string, object>.TryGetValue(string key, out object value)
        {
            if (this.Properties.TryGetValue(key, out PropertyInfo getter))
            {
                value = getter.GetValue(this);
                return true;
            }

            value = null;
            return false;
        }

        IEnumerator<KeyValuePair<string, object>> IEnumerable<KeyValuePair<string, object>>.GetEnumerator()
        {
            return this.Properties.Select(pair => new KeyValuePair<string, object>(pair.Key, pair.Value.GetValue(this))).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<KeyValuePair<string, object>>)this).GetEnumerator();
        }

        static IReadOnlyDictionary<string, PropertyInfo> GetProperties(Type type)
        {
            return SharedPropertiesCache.GetOrAdd(type, t =>
            {
                var properties = new Dictionary<string, PropertyInfo>();

                foreach (PropertyInfo property in t.GetProperties())
                {
                    StructuredLogFieldAttribute fieldAttribute = property.GetCustomAttribute<StructuredLogFieldAttribute>();
                    if (fieldAttribute != null)
                    {
                        if (!property.CanRead)
                        {
                            throw new InvalidOperationException($"Properties with {nameof(StructuredLogFieldAttribute)} must have a getter.");
                        }

                        string propertyName = fieldAttribute.Name ?? property.Name;
                        if (!properties.ContainsKey(propertyName))
                        {
                            properties[propertyName] = property;
                        }
                    }
                }

                return properties;
            });
        }

        /// <summary>
        /// Gets a log-friendly description of a history event.
        /// </summary>
        /// <param name="eventType">The type of history event in string-form.</param>
        /// <param name="taskEventId">The task event ID.</param>
        /// <returns>Returns <paramref name="eventType"/> and appends <paramref name="taskEventId"/> in parenthesis if it is a non-negative number - e.g. "TaskActivityScheduled(1)".</returns>
        protected static string GetEventDescription(string eventType, int taskEventId)
        {
            if (taskEventId >= 0)
            {
                return $"[{eventType}#{taskEventId}]";
            }
            else
            {
                return $"[{eventType}]";
            }
        }
    }
}
