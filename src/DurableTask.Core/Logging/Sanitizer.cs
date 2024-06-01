// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace DurableTask.Core.Logging
{
    /// <summary>
    /// Class
    /// </summary>
    internal static class Sanitizer
    {
        private static readonly List<string> regexList = new List<string>
        {
            @"(?i)\W[a-z0-9/\+]{86}==",
            @"(?-i)eyJ(?i)[a-z0-9\-_%]+\.(?-i)eyJ", // bearer token
        };

        internal static string Sanitize(this string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return string.Empty;
            }

            // Combine the regex patterns into a single pattern
            string combinedPattern = string.Join("|", regexList);

            // Regex match for value
            if (Regex.IsMatch(value, combinedPattern, RegexOptions.IgnoreCase))
            {
                value = "[Redacted]";
            }

            return value;
        }
    }
}
