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

namespace DurableTask.Core.Middleware
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Context data that can be used to share data between middleware.
    /// </summary>
    public class DispatchMiddlewareContext
    {
        internal DispatchMiddlewareContext()
        {
        }

        /// <summary>
        /// Sets a property value to the context using the full name of the type as the key.
        /// </summary>
        /// <typeparam name="T">The type of the property.</typeparam>
        /// <param name="value">The value of the property.</param>
        public void SetProperty<T>(T value)
        {
            this.SetProperty(typeof(T).FullName, value);
        }

        /// <summary>
        /// Sets a named property value to the context.
        /// </summary>
        /// <typeparam name="T">The type of the property.</typeparam>
        /// <param name="key">The name of the property.</param>
        /// <param name="value">The value of the property.</param>
        public void SetProperty<T>(string key, T value)
        {
            this.Properties[key] = value;
        }

        /// <summary>
        /// Gets a property value from the context using the full name of <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The type of the property.</typeparam>
        /// <returns>The value of the property or <c>default(T)</c> if the property is not defined.</returns>
        public T GetProperty<T>()
        {
            return this.GetProperty<T>(typeof(T).FullName);
        }

        /// <summary>
        /// Gets a named property value from the context.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The name of the property value.</param>
        /// <returns>The value of the property or <c>default(T)</c> if the property is not defined.</returns>
        public T GetProperty<T>(string key)
        {
            return this.Properties.TryGetValue(key, out object value) ? (T)value : default(T);
        }

        /// <summary>
        /// Gets a key/value collection that can be used to share data between middleware.
        /// </summary>
        public IDictionary<string, object> Properties { get; } = new Dictionary<string, object>(StringComparer.Ordinal);
    }
}
