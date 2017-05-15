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

namespace DurableTask.Core.Exceptions
{
    using System;

    /// <summary>
    /// Represents a type missing error
    /// </summary>
    [Serializable]
    public class TypeMissingException : Exception
    {
        /// <summary>
        /// Initializes an new instance of the TypeMissingException class with a specified error message
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public TypeMissingException(string message)
            : base(message)
        {
        }
    }
}