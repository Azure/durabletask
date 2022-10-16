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

namespace DurableTask.AzureStorage.Tracking
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Reflection;
    using System.Runtime.Serialization;
    using Azure.Data.Tables;
    using Newtonsoft.Json;

    /// <summary>
    /// Utility class for converting [DataContract] objects into DynamicTableEntity and back.
    /// This class makes heavy use of reflection to build the entity converters.
    /// </summary>
    /// <remarks>
    /// This class is safe for concurrent usage by multiple threads.
    /// </remarks>
    class TableEntityConverter
    {
        readonly ConcurrentDictionary<Type, IReadOnlyList<PropertyConverter>> converterCache;

        public TableEntityConverter()
        {
            this.converterCache = new ConcurrentDictionary<Type, IReadOnlyList<PropertyConverter>>();
        }

        /// <summary>
        /// Converts a data contract object into a <see cref="TableEntity"/>.
        /// </summary>
        public TableEntity ConvertToTableEntity(object obj)
        {
            if (obj == null)
            {
                throw new ArgumentNullException(nameof(obj));
            }

            Debug.Assert(obj.GetType().GetCustomAttribute<DataContractAttribute>() != null);

            IReadOnlyList<PropertyConverter> propertyConverters = this.converterCache.GetOrAdd(
                obj.GetType(),
                GetPropertyConvertersForType);

            var tableEntity = new TableEntity();
            foreach (PropertyConverter propertyConverter in propertyConverters)
            {
                tableEntity[propertyConverter.PropertyName] = propertyConverter.GetObjectProperty(obj);
            }

            return tableEntity;
        }

        public T ConvertFromTableEntity<T>(TableEntity tableEntity, Func<TableEntity, Type> typeFactory)
        {
            if (tableEntity == null)
            {
                throw new ArgumentNullException(nameof(tableEntity));
            }

            if (typeFactory == null)
            {
                throw new ArgumentNullException(nameof(typeFactory));
            }

            Type objectType = typeFactory(tableEntity);
            object createdObject = FormatterServices.GetUninitializedObject(objectType);

            IReadOnlyList<PropertyConverter> propertyConverters = this.converterCache.GetOrAdd(
                objectType,
                GetPropertyConvertersForType);

            foreach (PropertyConverter propertyConverter in propertyConverters)
            {
                // Properties with null values are not actually saved/retrieved by table storage.
                if (tableEntity.TryGetValue(propertyConverter.PropertyName, out object entityProperty))
                {
                    propertyConverter.SetObjectProperty(createdObject, entityProperty);
                }
            }

            return (T)createdObject;
        }

        static List<PropertyConverter> GetPropertyConvertersForType(Type type)
        {
            var propertyConverters = new List<PropertyConverter>();
            BindingFlags flags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly;

            // Loop through the type hierarchy to find all [DataMember] attributes which belong to [DataContract] classes.
            while (type != null && type.GetCustomAttribute<DataContractAttribute>() != null)
            {
                foreach (MemberInfo member in type.GetMembers(flags))
                {
                    DataMemberAttribute dataMember = member.GetCustomAttribute<DataMemberAttribute>();
                    if (dataMember == null)
                    {
                        continue;
                    }

                    PropertyInfo property = member as PropertyInfo;
                    FieldInfo field = member as FieldInfo;
                    if (property == null && field == null)
                    {
                        throw new InvalidDataContractException("Only fields and properties can be marked as [DataMember].");
                    }
                    else if (property != null && (!property.CanWrite || !property.CanRead))
                    {
                        throw new InvalidDataContractException("[DataMember] properties must be both readable and writeable.");
                    }

                    // Timestamp is a reserved property name in Table Storage, so the name needs to be changed.
                    string propertyName = dataMember.Name ?? member.Name;
                    if (string.Equals(propertyName, "Timestamp", StringComparison.OrdinalIgnoreCase))
                    {
                        propertyName = "_Timestamp";
                    }

                    Func<object, object> getEntityPropertyFunc;
                    Action<object, object> setObjectPropertyFunc;

                    Type memberValueType = property != null ? property.PropertyType : field.FieldType;
                    if (UseDefaultHandling(memberValueType))
                    {
                        if (property != null)
                        {
                            getEntityPropertyFunc = o => property.GetValue(o);
                            setObjectPropertyFunc = (o, e) => property.SetValue(o, e);
                        }
                        else
                        {
                            getEntityPropertyFunc = o => field.GetValue(o);
                            setObjectPropertyFunc = (o, e) => field.SetValue(o, e);
                        }
                    }
                    else if (memberValueType.IsEnum)
                    {
                        // Enums are serialized as strings for readability.
                        if (property != null)
                        {
                            getEntityPropertyFunc = o => property.GetValue(o).ToString();
                            setObjectPropertyFunc = (o, e) => property.SetValue(o, Enum.Parse(memberValueType, e as string));
                        }
                        else
                        {
                            getEntityPropertyFunc = o => field.GetValue(o).ToString();
                            setObjectPropertyFunc = (o, e) => field.SetValue(o, Enum.Parse(memberValueType, e as string));
                        }
                    }
                    else // assume a serializeable object
                    {
                        getEntityPropertyFunc = o =>
                        {
                            object value = property != null ? property.GetValue(o) : field.GetValue(o);
                            return value != null ? Utils.SerializeToJson(value) : null;
                        };

                        setObjectPropertyFunc = (o, e) =>
                        {
                            string json = e as string;
                            object value = json != null ? Utils.DeserializeFromJson(json, memberValueType) : null;
                            if (property != null)
                            {
                                property.SetValue(o, value);
                            }
                            else
                            {
                                field.SetValue(o, value);
                            }
                        };
                    }

                    propertyConverters.Add(new PropertyConverter(propertyName, getEntityPropertyFunc, setObjectPropertyFunc));
                }

                type = type.BaseType;
            }

            return propertyConverters;
        }

        static bool UseDefaultHandling(Type memberValueType)
        {
            return typeof(string).IsAssignableFrom(memberValueType) ||
                typeof(int?).IsAssignableFrom(memberValueType) ||
                typeof(long?).IsAssignableFrom(memberValueType) ||
                typeof(bool?).IsAssignableFrom(memberValueType) ||
                typeof(DateTime?).IsAssignableFrom(memberValueType) ||
                typeof(DateTimeOffset?).IsAssignableFrom(memberValueType) ||
                typeof(Guid?).IsAssignableFrom(memberValueType) ||
                typeof(double?).IsAssignableFrom(memberValueType) ||
                typeof(byte[]).IsAssignableFrom(memberValueType);
        }

        sealed class PropertyConverter
        {
            public PropertyConverter(
                string propertyName,
                Func<object, object> getObjectProperty,
                Action<object, object> setObjectProperty)
            {
                this.PropertyName = propertyName;
                this.GetObjectProperty = getObjectProperty;
                this.SetObjectProperty = setObjectProperty;
            }

            public string PropertyName { get; }

            public Func<object, object> GetObjectProperty { get; }

            public Action<object, object> SetObjectProperty { get; }
        }
    }
}
