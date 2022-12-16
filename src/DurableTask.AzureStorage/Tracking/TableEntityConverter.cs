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
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Runtime.Serialization;
    using Azure.Data.Tables;

    /// <summary>
    /// Utility class for converting objects that use <see cref="DataContractAttribute"/> into TableEntity and back.
    /// This class makes heavy use of reflection to build the entity converters.
    /// </summary>
    /// <remarks>
    /// This class is thread-safe.
    /// </remarks>
    class TableEntityConverter
    {
        static readonly ConcurrentDictionary<Type, Func<TableEntity, object>> DeserializeCache = new ConcurrentDictionary<Type, Func<TableEntity, object>>();
        static readonly ConcurrentDictionary<Type, Func<object, TableEntity>> SerializerCache = new ConcurrentDictionary<Type, Func<object, TableEntity>>();

        public static TableEntity Serialize(object obj) =>
            SerializerCache.GetOrAdd(obj?.GetType(), t => CreateTableEntitySerializer(t)).Invoke(obj);

        public static object Deserialize(TableEntity entity, Type type) =>
            DeserializeCache.GetOrAdd(type, t => CreateTableEntityDeserializer(t)).Invoke(entity);

        private static Func<TableEntity, object> CreateTableEntityDeserializer(Type type)
        {
            if (type == null)
            {
                throw new ArgumentNullException(nameof(type));
            }

            Debug.Assert(type.GetCustomAttribute<DataContractAttribute>() != null);

            MethodInfo getStringMethod = typeof(TableEntity).GetMethod(nameof(TableEntity.GetString), new Type[] { typeof(string) });
            MethodInfo parseMethod = typeof(Enum).GetMethod(nameof(Enum.Parse), new Type[] { typeof(Type), typeof(string) });
            MethodInfo deserializeMethod = typeof(Utils).GetMethod(nameof(Utils.DeserializeFromJson), new Type[] { typeof(string) });

            ParameterExpression tableParam = Expression.Variable(typeof(TableEntity), "table");
            ParameterExpression outputVar = Expression.Parameter(type, "output");
            var variables = new List<ParameterExpression> { outputVar };
            var body = new List<Expression>();

            #region if (table == null) throw new ArgumentNullException(nameof(table));
            body.Add(Expression.IfThen(
                Expression.Equal(tableParam, Expression.Constant(null, typeof(TableEntity))),
                Expression.Throw(
                    Expression.New(
                        typeof(ArgumentNullException).GetConstructor(new Type[] { typeof(string) }),
                        Expression.Constant(tableParam.Name, typeof(string))))));
            #endregion

            #region <Type> output = (<Type>)FormatterServices.GetUninitializedObject(typeof(<Type>));
            MethodInfo getUninitializedObjectMethod = typeof(FormatterServices).GetMethod(nameof(FormatterServices.GetUninitializedObject), new Type[] { typeof(Type) });
            body.Add(Expression.Assign(
                outputVar,
                Expression.Convert(
                    Expression.Call(null, getUninitializedObjectMethod, Expression.Constant(type, typeof(Type))),
                    type)));
            #endregion

            foreach ((string propertyName, Type memberType, MemberInfo metadata) in EnumerateMembers(type))
            {
                #region output.<Member> = /* ... */
                Expression valueExpr;
                if (memberType.IsEnum)
                {
                    #region string <Member>Variable = table.GetString("<property>");
                    ParameterExpression enumStringVar = Expression.Parameter(typeof(string), propertyName + "Variable");
                    variables.Add(enumStringVar);

                    body.Add(Expression.Assign(enumStringVar, Expression.Call(tableParam, getStringMethod, Expression.Constant(propertyName, typeof(string)))));
                    #endregion

                    #region output.<Member> = <Member>Variable == null ? default(<MemberType>) : (<MemberType>)Enum.Parse(typeof(<MemberType>), <Member>Variable);
                    valueExpr = Expression.Condition(
                        Expression.Equal(enumStringVar, Expression.Constant(null, typeof(string))),
                        Expression.Default(memberType),
                        Expression.Convert(Expression.Call(null, parseMethod, Expression.Constant(memberType, typeof(Type)), enumStringVar), memberType));
                    #endregion
                }
                else if(IsSupportedType(memberType))
                {
                    #region output.<Member> = table.Get<MemberType>("<property>");
                    MethodInfo accessorMethod = GetEntityAccessor(memberType);
                    valueExpr = Expression.Call(tableParam, accessorMethod, Expression.Constant(propertyName, typeof(string)));
                    #endregion

                    if (memberType.IsValueType && !memberType.IsGenericType) // Cannot be null
                    {
                        #region output.<Member> = table.Get<MemberType>("<property>").GetValueOrDefault();
                        valueExpr = Expression.Call(valueExpr, typeof(Nullable<>).MakeGenericType(memberType).GetMethod(nameof(Nullable<int>.GetValueOrDefault), Type.EmptyTypes));
                        #endregion
                    }
                }
                else
                {
                    // Note: We also deserialize nullable enumerations using JSON for backwards compatibility
                    #region string <Member>Variable = table.GetString("<property>");
                    ParameterExpression jsonVariable = Expression.Parameter(typeof(string), propertyName + "Variable");
                    variables.Add(jsonVariable);

                    body.Add(Expression.Assign(jsonVariable, Expression.Call(tableParam, getStringMethod, Expression.Constant(propertyName, typeof(string)))));
                    #endregion

                    #region output.<Member> = = <Member>Variable == null ? default(<MemberType>) : Utils.DeserializeFromJson<<MemberType>>(<Member>Variable);
                    valueExpr = Expression.Condition(
                        Expression.Equal(jsonVariable, Expression.Constant(null, typeof(string))),
                        Expression.Default(memberType),
                        Expression.Call(null, deserializeMethod.MakeGenericMethod(memberType), jsonVariable));
                    #endregion
                }

                body.Add(Expression.Assign(Expression.MakeMemberAccess(outputVar, metadata), valueExpr));
                #endregion
            }

            #region return (object)output;
            body.Add(type != typeof(object) ? Expression.Convert(outputVar, type) : outputVar);
            #endregion

            return Expression.Lambda<Func<TableEntity, object>>(Expression.Block(variables, body), tableParam).Compile();
        }

        private static Func<object, TableEntity> CreateTableEntitySerializer(Type type)
        {
            if (type == null)
            {
                throw new ArgumentNullException(nameof(type));
            }

            Debug.Assert(type.GetCustomAttribute<DataContractAttribute>() != null);

            // Indexers use "get_Item" and "set_Item"
            MethodInfo setItemMethod = typeof(TableEntity).GetMethod("set_Item", new Type[] { typeof(string), typeof(object) });
            MethodInfo serializeMethod = typeof(Utils).GetMethod(nameof(Utils.SerializeToJson), new Type[] { typeof(string) });

            ParameterExpression objParam = Expression.Parameter(typeof(object), "obj");
            ParameterExpression inputVar = Expression.Variable(type, "input");
            ParameterExpression tableVar = Expression.Variable(typeof(TableEntity), "table");
            var variables = new List<ParameterExpression> { inputVar, tableVar };
            var body = new List<Expression>();

            #region if (obj == null) throw new ArgumentNullException(nameof(obj));
            body.Add(Expression.IfThen(
                Expression.Equal(objParam, Expression.Constant(null, typeof(object))),
                Expression.Throw(
                    Expression.New(
                        typeof(ArgumentNullException).GetConstructor(new Type[] { typeof(string) }),
                        Expression.Constant(objParam.Name, typeof(string))))));
            #endregion

            #region <Type> input = (<Type>)obj;
            body.Add(Expression.Assign(inputVar, type != typeof(object) ? Expression.Convert(objParam, type) : objParam));
            #endregion

            #region TableEntity table = new TableEntity();
            body.Add(Expression.Assign(tableVar, Expression.New(typeof(TableEntity))));
            #endregion

            foreach ((string propertyName, Type memberType, MemberInfo metadata) in EnumerateMembers(type))
            {
                #region table["<property>"] = (object)/* ... */
                Expression valueExpr;
                MemberExpression memberExpr = Expression.MakeMemberAccess(inputVar, metadata);
                if (memberType.IsEnum)
                {
                    #region table["<property>"] = input.<Member>.ToString("G");
                    MethodInfo toStringMethod = memberType.GetMethod(nameof(object.ToString), new Type[] { typeof(string) });
                    valueExpr = Expression.Call(memberExpr, toStringMethod, Expression.Constant("G", typeof(string)));
                    #endregion
                }
                else if (IsSupportedType(memberType))
                {
                    #region table["<property>"] = input.<Member>;
                    valueExpr = memberExpr;
                    #endregion
                }
                else
                {
                    // Note: We also serialize nullable enumerations using JSON for backwards compatibility
                    #region table["<property>"] = Utils.SerializeToJson(input.<Member>);
                    valueExpr = Expression.Call(null, serializeMethod, memberType != typeof(object) ? Expression.Convert(memberExpr, typeof(object)) : memberExpr);
                    #endregion
                }

                body.Add(Expression.Call(tableVar, setItemMethod, Expression.Constant(propertyName, typeof(string)), Expression.Convert(valueExpr, typeof(object))));
                #endregion
            }

            #region return table;
            body.Add(tableVar);
            #endregion

            return Expression.Lambda<Func<object, TableEntity>>(Expression.Block(variables, body), objParam).Compile();
        }

        static IEnumerable<(string Name, Type MemberType, MemberInfo Metadata)> EnumerateMembers(Type type)
        {
            const BindingFlags Flags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly;

            // Loop through the type hierarchy to find all [DataMember] attributes which belong to [DataContract] classes.
            while (type != null && type.GetCustomAttribute<DataContractAttribute>() != null)
            {
                foreach (MemberInfo member in type.GetMembers(Flags))
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
                    else if ((property != null && (!property.CanWrite || !property.CanRead)) || (field != null && field.IsInitOnly))
                    {
                        throw new InvalidDataContractException("[DataMember] properties must be both readable and writeable.");
                    }

                    // Timestamp is a reserved property name in Table Storage, so the name needs to be changed.
                    string name = dataMember.Name ?? member.Name;
                    if (string.Equals(name, "Timestamp", StringComparison.OrdinalIgnoreCase))
                    {
                        name = "_Timestamp";
                    }

                    yield return (name, property != null ? property.PropertyType : field.FieldType, member);
                }

                type = type.BaseType;
            }
        }

        // TODO: Add support for BinaryData if necessary
        static bool IsSupportedType(Type type) =>
            type == typeof(string) ||
            type == typeof(byte[]) ||
            type == typeof(bool) ||
            type == typeof(bool?) ||
            type == typeof(DateTime) ||
            type == typeof(DateTime?) ||
            type == typeof(DateTimeOffset) ||
            type == typeof(DateTimeOffset?) ||
            type == typeof(double) ||
            type == typeof(double?) ||
            type == typeof(Guid) ||
            type == typeof(Guid?) ||
            type == typeof(int) ||
            type == typeof(int?) ||
            type == typeof(long) ||
            type == typeof(long?);

        static MethodInfo GetEntityAccessor(Type type)
        {
            if (type == typeof(string))
                return typeof(TableEntity).GetMethod(nameof(TableEntity.GetString), new Type[] { typeof(string) });

            if (type == typeof(byte[]))
                return typeof(TableEntity).GetMethod(nameof(TableEntity.GetBinary), new Type[] { typeof(string) });

            if (type == typeof(bool) || type == typeof(bool?))
                return typeof(TableEntity).GetMethod(nameof(TableEntity.GetBoolean), new Type[] { typeof(string) });

            if (type == typeof(DateTime) || type == typeof(DateTime?))
                return typeof(TableEntity).GetMethod(nameof(TableEntity.GetDateTime), new Type[] { typeof(string) });

            if (type == typeof(DateTimeOffset) || type == typeof(DateTimeOffset?))
                return typeof(TableEntity).GetMethod(nameof(TableEntity.GetDateTimeOffset), new Type[] { typeof(string) });

            if (type == typeof(double) || type == typeof(double?))
                return typeof(TableEntity).GetMethod(nameof(TableEntity.GetDouble), new Type[] { typeof(string) });

            if (type == typeof(Guid) || type == typeof(Guid?))
                return typeof(TableEntity).GetMethod(nameof(TableEntity.GetGuid), new Type[] { typeof(string) });

            if (type == typeof(int) || type == typeof(int?))
                return typeof(TableEntity).GetMethod(nameof(TableEntity.GetInt32), new Type[] { typeof(string) });

            return typeof(TableEntity).GetMethod(nameof(TableEntity.GetInt64), new Type[] { typeof(string) });
        }
    }
}
