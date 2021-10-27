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
    using System.Text;

    static class KeySanitation
    {
        private const char escapeChar = '^';
        private const char offset = 'À';

        /// <summary>
        /// Escape any characters that can't be used in Azure PartitionKey.
        /// https://docs.microsoft.com/en-us/rest/api/storageservices/Understanding-the-Table-Service-Data-Model?redirectedfrom=MSDN
        /// </summary>
        /// <param name="key"></param>
        /// <returns>The modified string.</returns>
        public static string EscapePartitionKey(string key)
        {
            if (key == null)
            {
                return null;
            }

            StringBuilder b = new StringBuilder();
            foreach (char c in key)
            {
                switch (c)
                {
                    case escapeChar:
                        b.Append(escapeChar); 
                        b.Append(escapeChar);
                        break;

                    case '/':
                        b.Append(escapeChar);
                        b.Append('0');
                        break;

                    case '\\': 
                        b.Append(escapeChar);
                        b.Append('1'); 
                        break;

                    case '#': 
                        b.Append(escapeChar); 
                        b.Append('2'); 
                        break;

                    case '?': 
                        b.Append(escapeChar); 
                        b.Append('3');
                        break;

                    default:
                        {
                            uint val = (uint)c;

                            if (val <= 0x1F || (val >= 0x7F && val <= 0x9F))
                            {
                                // characters in this range cannot be used, so we escape them 
                                b.Append(escapeChar);
                                // and shift them into a valid (unicode) range
                                b.Append((char) (offset + val));
                            }
                            else
                            {
                                b.Append(c);
                            }

                            break;
                        }
                }
            }
            return b.ToString();
        }

        /// <summary>
        /// Unescape characters that were previously escaped.
        /// </summary>
        /// <param name="key"></param>
        /// <returns>The original string.</returns>
        public static string UnescapePartitionKey(string key)
        {
            if (key == null)
            {
                return null;
            }

            StringBuilder b = new StringBuilder();
            for (int i = 0; i < key.Length; i++)
            {
                char c = key[i];
                if (c != escapeChar)
                {
                    b.Append(c);
                }
                else
                {
                    c = key[++i];
                    switch (c)
                    {
                        case escapeChar: b.Append(escapeChar); break;

                        case '0':
                            b.Append('/');
                            break;

                        case '1': 
                            b.Append('\\');
                            break;

                        case '2': 
                            b.Append('#');
                            break;

                        case '3': 
                            b.Append('?'); 
                            break;

                        default:
                            {
                                char shiftedBack = (c < offset) ? c : (char)(c - offset);
                                b.Append(shiftedBack);
                                break;
                            }
                    }
                }
            }
            return b.ToString();
        }
    }
}
