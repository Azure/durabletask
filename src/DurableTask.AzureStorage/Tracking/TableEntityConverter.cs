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
    using Newtonsoft.Json;

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

            #region <Type> output = new <Type>();
            body.Add(Expression.Assign(outputVar, Expression.New(type)));
            #endregion

            foreach ((string propertyName, Type memberType, MemberInfo metadata) in EnumerateMembers(type))
            {
                #region output.<Member> = /* ... */
                Expression valueExpr;
                if (memberType.IsEnum || IsNullableEnum(memberType))
                {
                    #region string <Member>Variable = table.GetString("<property>");
                    ParameterExpression enumStringVar = Expression.Parameter(typeof(string), propertyName + "Variable");
                    variables.Add(enumStringVar);

                    body.Add(Expression.Assign(enumStringVar, Expression.Call(tableParam, typeof(TableEntity).GetMethod(nameof(TableEntity.GetString)), Expression.Constant(propertyName, typeof(string)))));
                    #endregion

                    #region output.<Member> = <Member>Variable == null ? default(<MemberType>) : (<MemberType>)Enum.Parse(typeof(<MemberType>), <Member>Variable);
                    Type enumType = memberType.IsEnum ? memberType : memberType.GetGenericArguments()[0];
                    valueExpr = Expression.Condition(
                        Expression.Equal(enumStringVar, Expression.Constant(null, typeof(string))),
                        Expression.Default(memberType),
                        Expression.Convert(Expression.Call(null, parseMethod, Expression.Constant(enumType, typeof(Type)), enumStringVar), memberType));
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
                    #region string <Member>Variable = table.GetString("<property>");
                    ParameterExpression jsonVariable = Expression.Parameter(typeof(string), propertyName + "Variable");
                    variables.Add(jsonVariable);

                    body.Add(Expression.Assign(jsonVariable, Expression.Call(tableParam, typeof(TableEntity).GetMethod(nameof(TableEntity.GetString)), Expression.Constant(propertyName, typeof(string)))));
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

            MethodInfo setItemMethod = typeof(TableEntity).GetMethod("set_Item"); // Indexers use "get_Item" and "set_Item"
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
                else if (IsNullableEnum(memberType))
                {
                    #region table["<property>"] = input.<Member> == null ? (string)null : input.<Member>.GetValueOrDefault().ToString("G");
                    MethodInfo getValueOrDefault = memberType.GetMethod(nameof(Nullable<int>.GetValueOrDefault), Type.EmptyTypes);
                    MethodInfo toStringMethod = memberType.GetGenericArguments()[0].GetMethod(nameof(object.ToString), new Type[] { typeof(string) });
                    valueExpr = Expression.Condition(
                        Expression.Equal(memberExpr, Expression.Constant(null, memberType)),
                        Expression.Constant(null, typeof(string)),
                        Expression.Call(Expression.Call(memberExpr, getValueOrDefault), toStringMethod, Expression.Constant("G", typeof(string))));
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

            var lambda = Expression.Lambda<Func<object, TableEntity>>(Expression.Block(variables, body), objParam);
            return lambda.Compile();
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

        static bool IsNullableEnum(Type type) =>
            type.IsGenericType &&
            type.GetGenericTypeDefinition() == typeof(Nullable<>) &&
            type.GetGenericArguments()[0].IsEnum;

        static bool IsSupportedType(Type type) =>
            type == typeof(string) ||
            type == typeof(BinaryData) ||
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
                return typeof(TableEntity).GetMethod(nameof(TableEntity.GetString));

            if (type == typeof(BinaryData))
                return typeof(TableEntity).GetMethod(nameof(TableEntity.GetBinaryData));

            if (type == typeof(byte[]))
                return typeof(TableEntity).GetMethod(nameof(TableEntity.GetBinary));

            if (type == typeof(bool) || type == typeof(bool?))
                return typeof(TableEntity).GetMethod(nameof(TableEntity.GetBoolean));

            if (type == typeof(DateTime) || type == typeof(DateTime?))
                return typeof(TableEntity).GetMethod(nameof(TableEntity.GetDateTime));

            if (type == typeof(DateTimeOffset) || type == typeof(DateTimeOffset?))
                return typeof(TableEntity).GetMethod(nameof(TableEntity.GetDateTimeOffset));

            if (type == typeof(double) || type == typeof(double?))
                return typeof(TableEntity).GetMethod(nameof(TableEntity.GetDouble));

            if (type == typeof(Guid) || type == typeof(Guid?))
                return typeof(TableEntity).GetMethod(nameof(TableEntity.GetGuid));

            if (type == typeof(int) || type == typeof(int?))
                return typeof(TableEntity).GetMethod(nameof(TableEntity.GetInt32));

            return typeof(TableEntity).GetMethod(nameof(TableEntity.GetInt64));
        }
    }
}
