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

using System;
using System.Collections.Generic;

namespace DurableTask.Core
{
    /// <summary>
    /// Extension methods that help get properties from <see cref="IContextProperties"/>.
    /// </summary>
    public static class ContextPropertiesExtensions
    {
        /// <summary>
        /// Sets a property value to the context using the full name of the type as the key.
        /// </summary>
        /// <typeparam name="T">The type of the property.</typeparam>
        /// <param name="properties">Properties to set property for.</param>
        /// <param name="value">The value of the property.</param>
        public static void SetProperty<T>(this IContextProperties properties, T? value) => properties.SetProperty(typeof(T).FullName, value);

        /// <summary>
        /// Sets a named property value to the context.
        /// </summary>
        /// <typeparam name="T">The type of the property.</typeparam>
        /// <param name="properties">Properties to set property for.</param>
        /// <param name="key">The name of the property.</param>
        /// <param name="value">The value of the property.</param>
        public static void SetProperty<T>(this IContextProperties properties, string key, T? value)
        {
            if (value is null)
            {
                properties.Properties.Remove(key);
            }
            else
            {
                properties.Properties[key] = value;
            }
        }

        /// <summary>
        /// Gets a property value from the context using the full name of <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The type of the property.</typeparam>
        /// <param name="properties">Properties to get property from.</param>
        /// <returns>The value of the property or <c>default(T)</c> if the property is not defined.</returns>
        public static T? GetProperty<T>(this IContextProperties properties) => properties.GetProperty<T>(typeof(T).FullName);

        internal static T GetRequiredProperty<T>(this IContextProperties properties)
            => properties.GetProperty<T>() ?? throw new InvalidOperationException($"Could not find property for {typeof(T).FullName}");

        /// <summary>
        /// Gets a named property value from the context.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="properties">Properties to get property from.</param>
        /// <param name="key">The name of the property value.</param>
        /// <returns>The value of the property or <c>default(T)</c> if the property is not defined.</returns>
        public static T? GetProperty<T>(this IContextProperties properties, string key) => properties.Properties.TryGetValue(key, out object value) ? (T)value : default;

        /// <summary>
        /// Gets the tags from the current properties.
        /// </summary>
        /// <param name="properties"></param>
        /// <returns></returns>
        public static IDictionary<string, string> GetTags(this IContextProperties properties) => properties.GetRequiredProperty<OrchestrationExecutionContext>().OrchestrationTags;
    }
}