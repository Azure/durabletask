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

namespace DurableTask.AzureStorage
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Reflection;
    using System.Runtime.Serialization;
    using Microsoft.WindowsAzure.Storage.Table;
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
        /// Converts a data contract object into a <see cref="DynamicTableEntity"/>.
        /// </summary>
        public DynamicTableEntity ConvertToTableEntity(object obj)
        {
            if (obj == null)
            {
                throw new ArgumentNullException(nameof(obj));
            }

            Debug.Assert(obj.GetType().GetCustomAttribute<DataContractAttribute>() != null);

            IReadOnlyList<PropertyConverter> propertyConverters = this.converterCache.GetOrAdd(
                obj.GetType(),
                GetPropertyConvertersForType);

            var tableEntity = new DynamicTableEntity();
            foreach (PropertyConverter propertyConverter in propertyConverters)
            {
                tableEntity.Properties[propertyConverter.PropertyName] = propertyConverter.GetEntityProperty(obj);
            }

            return tableEntity;
        }

        public object ConvertFromTableEntity(DynamicTableEntity tableEntity, Func<DynamicTableEntity, Type> typeFactory)
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
                EntityProperty entityProperty;
                if (tableEntity.Properties.TryGetValue(propertyConverter.PropertyName, out entityProperty))
                {
                    propertyConverter.SetObjectProperty(createdObject, entityProperty);
                }
            }

            return createdObject;
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

                    Func<object, EntityProperty> getEntityPropertyFunc;
                    Action<object, EntityProperty> setObjectPropertyFunc;

                    Type memberValueType = property != null ? property.PropertyType : field.FieldType;
                    if (typeof(string).IsAssignableFrom(memberValueType))
                    {
                        if (property != null)
                        {
                            getEntityPropertyFunc = o => EntityProperty.GeneratePropertyForString((string)property.GetValue(o));
                            setObjectPropertyFunc = (o, e) => property.SetValue(o, e.StringValue);
                        }
                        else
                        {
                            getEntityPropertyFunc = o => EntityProperty.GeneratePropertyForString((string)field.GetValue(o));
                            setObjectPropertyFunc = (o, e) => field.SetValue(o, e.StringValue);
                        }
                    }
                    else if (memberValueType.IsEnum)
                    {
                        // Enums are serialized as strings for readability.
                        if (property != null)
                        {
                            getEntityPropertyFunc = o => EntityProperty.GeneratePropertyForString(property.GetValue(o).ToString());
                            setObjectPropertyFunc = (o, e) => property.SetValue(o, Enum.Parse(memberValueType, e.StringValue));
                        }
                        else
                        {
                            getEntityPropertyFunc = o => EntityProperty.GeneratePropertyForString(field.GetValue(o).ToString());
                            setObjectPropertyFunc = (o, e) => field.SetValue(o, Enum.Parse(memberValueType, e.StringValue));
                        }
                    }
                    else if (typeof(int?).IsAssignableFrom(memberValueType))
                    {
                        if (property != null)
                        {
                            getEntityPropertyFunc = o => EntityProperty.GeneratePropertyForInt((int)property.GetValue(o));
                            setObjectPropertyFunc = (o, e) => property.SetValue(o, e.Int32Value);
                        }
                        else
                        {
                            getEntityPropertyFunc = o => EntityProperty.GeneratePropertyForInt((int)field.GetValue(o));
                            setObjectPropertyFunc = (o, e) => field.SetValue(o, e.Int32Value);
                        }
                    }
                    else if (typeof(long?).IsAssignableFrom(memberValueType))
                    {
                        if (property != null)
                        {
                            getEntityPropertyFunc = o => EntityProperty.GeneratePropertyForLong((long)property.GetValue(o));
                            setObjectPropertyFunc = (o, e) => property.SetValue(o, e.Int64Value);
                        }
                        else
                        {
                            getEntityPropertyFunc = o => EntityProperty.GeneratePropertyForLong((long)field.GetValue(o));
                            setObjectPropertyFunc = (o, e) => field.SetValue(o, e.Int64Value);
                        }
                    }
                    else if (typeof(bool?).IsAssignableFrom(memberValueType))
                    {
                        if (property != null)
                        {
                            getEntityPropertyFunc = o => EntityProperty.GeneratePropertyForBool((bool)property.GetValue(o));
                            setObjectPropertyFunc = (o, e) => property.SetValue(o, e.BooleanValue);
                        }
                        else
                        {
                            getEntityPropertyFunc = o => EntityProperty.GeneratePropertyForBool((bool)field.GetValue(o));
                            setObjectPropertyFunc = (o, e) => field.SetValue(o, e.BooleanValue);
                        }
                    }
                    else if (typeof(DateTime?).IsAssignableFrom(memberValueType))
                    {
                        if (property != null)
                        {
                            getEntityPropertyFunc = o => EntityProperty.GeneratePropertyForDateTimeOffset((DateTime?)property.GetValue(o));
                            setObjectPropertyFunc = (o, e) => property.SetValue(o, e.DateTime);
                        }
                        else
                        {
                            getEntityPropertyFunc = o => EntityProperty.GeneratePropertyForDateTimeOffset((DateTime?)field.GetValue(o));
                            setObjectPropertyFunc = (o, e) => field.SetValue(o, e.DateTime);
                        }
                    }
                    else if (typeof(DateTimeOffset?).IsAssignableFrom(memberValueType))
                    {
                        if (property != null)
                        {
                            getEntityPropertyFunc = o => EntityProperty.GeneratePropertyForDateTimeOffset((DateTimeOffset?)property.GetValue(o));
                            setObjectPropertyFunc = (o, e) => property.SetValue(o, e.DateTimeOffsetValue);
                        }
                        else
                        {
                            getEntityPropertyFunc = o => EntityProperty.GeneratePropertyForDateTimeOffset((DateTimeOffset?)field.GetValue(o));
                            setObjectPropertyFunc = (o, e) => field.SetValue(o, e.DateTimeOffsetValue);
                        }
                    }
                    else if (typeof(Guid?).IsAssignableFrom(memberValueType))
                    {
                        if (property != null)
                        {
                            getEntityPropertyFunc = o => EntityProperty.GeneratePropertyForGuid((Guid?)property.GetValue(o));
                            setObjectPropertyFunc = (o, e) => property.SetValue(o, e.GuidValue);
                        }
                        else
                        {
                            getEntityPropertyFunc = o => EntityProperty.GeneratePropertyForGuid((Guid?)field.GetValue(o));
                            setObjectPropertyFunc = (o, e) => field.SetValue(o, e.GuidValue);
                        }
                    }
                    else if (typeof(double?).IsAssignableFrom(memberValueType))
                    {
                        if (property != null)
                        {
                            getEntityPropertyFunc = o => EntityProperty.GeneratePropertyForDouble((double?)property.GetValue(o));
                            setObjectPropertyFunc = (o, e) => property.SetValue(o, e.DoubleValue);
                        }
                        else
                        {
                            getEntityPropertyFunc = o => EntityProperty.GeneratePropertyForDouble((double?)field.GetValue(o));
                            setObjectPropertyFunc = (o, e) => field.SetValue(o, e.DoubleValue);
                        }
                    }
                    else if (typeof(byte[]).IsAssignableFrom(memberValueType))
                    {
                        if (property != null)
                        {
                            getEntityPropertyFunc = o => EntityProperty.GeneratePropertyForByteArray((byte[])property.GetValue(o));
                            setObjectPropertyFunc = (o, e) => property.SetValue(o, e.BinaryValue);
                        }
                        else
                        {
                            getEntityPropertyFunc = o => EntityProperty.GeneratePropertyForByteArray((byte[])field.GetValue(o));
                            setObjectPropertyFunc = (o, e) => field.SetValue(o, e.BinaryValue);
                        }
                    }
                    else // assume a serializeable object
                    {
                        getEntityPropertyFunc = o =>
                        {
                            object value = property != null ? property.GetValue(o) : field.GetValue(o);
                            string json = value != null ? JsonConvert.SerializeObject(value) : null;
                            return EntityProperty.GeneratePropertyForString(json);
                        };

                        setObjectPropertyFunc = (o, e) =>
                        {
                            string json = e.StringValue;
                            object value = json != null ? JsonConvert.DeserializeObject(json, memberValueType) : null;
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

        class PropertyConverter
        {
            public PropertyConverter(
                string propertyName,
                Func<object, EntityProperty> toEntityPropertyConverter,
                Action<object, EntityProperty> toObjectPropertyConverter)
            {
                this.PropertyName = propertyName;
                this.GetEntityProperty = toEntityPropertyConverter;
                this.SetObjectProperty = toObjectPropertyConverter;
            }

            public string PropertyName { get; private set; }
            public Func<object, EntityProperty> GetEntityProperty { get; private set; }
            public Action<object, EntityProperty> SetObjectProperty { get; private set; }
        }
    }
}
