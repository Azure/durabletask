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

namespace DurableTask.Core.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.Serialization;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class CustomExceptionsTests
    {
        List<Type> customExceptions;
        const string DefaultExceptionsNamespace = "DurableTask.Core.Exceptions";
        const string DefaultExceptionMessage = "Test Message";
        const long TestInt64Type = 1234567890123456789;
        const int TestInt32Type = 1234567890;
        const short TestInt16Type = 12345;
        const string TestStringType = "This is a test string";
        static readonly Guid TestGuidType = Guid.NewGuid();

        [TestInitialize]
        public void Initialize()
        {
            // Get all exceptions from the DurableTask.Core that are public
            this.customExceptions = typeof(TaskHubWorker).Assembly.GetTypes().Where(_ => typeof(Exception).IsAssignableFrom(_) && _.IsPublic).ToList();
        }

        [TestMethod]
        public void CustomExceptionNamespace()
        {
            this.customExceptions.ForEach(_ =>
            {
                Assert.AreEqual(DefaultExceptionsNamespace, _.Namespace, "All custom exception must be defined in the '{DefaultExceptionsNamespace}' namespace");
            });
        }

        [TestMethod]
        public void CustomExceptionDefaultConstructors()
        {
            this.customExceptions.ForEach(_ =>
            {
                // Get the default constructor
                ConstructorInfo constructor = _.GetConstructor(BindingFlags.Instance | BindingFlags.Public, null, CallingConventions.HasThis, new Type[0], null);
                Assert.IsNotNull(constructor, $"Default constructor .ctor() for exception '{_.FullName}' does not exist");

                // Create an instance to make sure no exception is raised
                constructor.Invoke(new object[0]);

                // Get the constructor with a single string parameter
                constructor = _.GetConstructor(BindingFlags.Instance | BindingFlags.Public, null, CallingConventions.HasThis, new[] { typeof(string) }, null);
                Assert.IsNotNull(constructor, $"Default constructor .ctor(string) for exception '{_.FullName}' does not exist");

                // Create an instance to make sure no exception is raised
                var exception = (Exception)constructor.Invoke(new object[] { DefaultExceptionMessage });
                Assert.AreEqual(DefaultExceptionMessage, exception.Message);

                // Get the constructor with a single string parameter and a inner exception
                constructor = _.GetConstructor(BindingFlags.Instance | BindingFlags.Public, null, CallingConventions.HasThis, new[] { typeof(string), typeof(Exception) }, null);
                Assert.IsNotNull(constructor, $"Default constructor .ctor(string, Exception) for exception '{_.FullName}' does not exist");

                // Create an instance to make sure no exception is raised
                var timeOutException = new TimeoutException();
                exception = (Exception)constructor.Invoke(new object[] { DefaultExceptionMessage, timeOutException });
                Assert.AreSame(timeOutException, exception.InnerException, $"Inner exception for exception '{_.FullName}' was not set properly");
            });
        }

        [TestMethod]
        public void CustomExceptionSerialization()
        {
            var ignoredProperties = new Dictionary<string, Type>
            {
                { "ClassName", typeof(string) },
                { "Message", typeof(string) },
                { "InnerException", typeof(ArgumentException) },
                { "HelpURL", typeof(string) },
                { "StackTraceString", typeof(string) },
                { "RemoteStackTraceString", typeof(string) },
                { "RemoteStackIndex", typeof(int) },
                { "ExceptionMethod", typeof(string) },
                { "HResult", typeof(int) },
                { "Source", typeof(string) },
                { "Data", typeof(Dictionary<string, object>) }
            };

            ForEachExceptionDisplayContext(_ =>
            {
                // Get the default constructor
                ConstructorInfo constructor = _.GetConstructor(BindingFlags.Instance | BindingFlags.NonPublic, null, CallingConventions.HasThis, new[] { typeof(SerializationInfo), typeof(StreamingContext) }, null);
                Assert.IsNotNull(constructor, $"Constructor .ctor(SerializationInfo, StreamingContext) for exception '{_.FullName}' does not exist");
                Assert.AreEqual(false, constructor.IsPrivate, $"Constructor .ctor(SerializationInfo, StreamingContext) for exception '{_.FullName}' must be protected");

                var streamingContext = new StreamingContext();

                var info = new SerializationInfo(_, new FormatterConverter());

                // ReSharper disable once AccessToModifiedClosure
                // Add base properties
                ignoredProperties.ToList().ForEach(keyValuePair => info.AddValue(keyValuePair.Key, GetValueFromType(keyValuePair.Value)));

                // Get the properties defined in this exception
                PropertyInfo[] properties = _.GetProperties(BindingFlags.Instance | BindingFlags.Public)
                    .Where(property => 
                        ignoredProperties.ContainsKey(property.Name) == false &&
                        property.DeclaringType != typeof(Exception))
                    .ToArray();

                foreach (PropertyInfo propertyInfo in properties)
                {
                    // Set a value for each property defined in the exception to be added during serialization
                    Console.WriteLine($"Adding property {propertyInfo.Name}");
                    info.AddValue(propertyInfo.Name, GetValueFromType(propertyInfo.PropertyType));
                }

                // Create an instance to make sure no exception is raised
                var exception = (Exception)constructor.Invoke(new object[] { info, streamingContext });

                // Make sure the values were serialized property
                info = new SerializationInfo(_, new FormatterConverter());
                exception.GetObjectData(info, streamingContext);

                foreach (PropertyInfo propertyInfo in properties)
                {
                    // Get the value of the exception property added during serialization
                    Console.WriteLine($"Validating property {propertyInfo.Name}");
                    object value = info.GetValue(propertyInfo.Name, propertyInfo.PropertyType);
                    Assert.AreEqual(GetValueFromType(propertyInfo.PropertyType), value);
                }
            });
        }

        void ForEachExceptionDisplayContext(Action<Type> exceptionType)
        {
            foreach (Type type in this.customExceptions)
            {
                Console.WriteLine($"Validating type {type}...");
                exceptionType(type);
            }
        }

        object GetValueFromType(Type type)
        {
            // Return a predefined value for each well known type
            if (type == typeof(long)) return TestInt64Type;
            if (type == typeof(int)) return TestInt32Type;
            if (type == typeof(short)) return TestInt16Type;
            if (type == typeof(string)) return TestStringType;
            if (type == typeof(Guid)) return TestGuidType;

            return Activator.CreateInstance(type);
        }
    }
}
