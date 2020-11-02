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

namespace DurableTask.AzureStorage.Logging
{
    using System;
    using Microsoft.Extensions.Logging;

    // There is a NullLoggerFactory defined in Microsoft.Extensions.Logging.Abstractions v2.x and above, but we 
    // cannot rely on it because Azure Functions v1 is pinned to Microsoft.Extensions.Logging.Abstractions v1.x.
    sealed class NoOpLoggerFactory : ILoggerFactory
    {
        public static readonly NoOpLoggerFactory Instance = new NoOpLoggerFactory();

        NoOpLoggerFactory()
        {
        }

        public void AddProvider(ILoggerProvider provider)
        {
        }

        public ILogger CreateLogger(string categoryName)
        {
            return NoOpLogger.Instance;
        }

        public void Dispose()
        {
        }

        sealed class NoOpLogger : ILogger, IDisposable
        {
            public static NoOpLogger Instance = new NoOpLogger();

            public IDisposable BeginScope<TState>(TState state) => this;

            public bool IsEnabled(LogLevel logLevel) => false;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
            {
            }

            void IDisposable.Dispose()
            {
            }
        }
    }
}