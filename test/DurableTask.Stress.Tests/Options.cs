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

namespace DurableTask.Stress.Tests
{
    using CommandLine;
    using CommandLine.Text;

    internal class Options
    {
#if NETCOREAPP2_1
        [Option('c', "create-hub", Default = false,
            HelpText = "Create Orchestration Hub.")]
        public bool CreateHub { get; set; }

        [Option('s', "start-instance", Default = null,
            HelpText = "Start Driver Instance")]
        public string StartInstance { get; set; }

        [Option('i', "instance-id",
            HelpText = "Instance id for new orchestration instance.")]
        public string InstanceId { get; set; }

        public static string GetUsage(ParserResult<Options> options)
        {
            // this without using CommandLine.Text
            //  or using HelpText.AutoBuild

            var help = new HelpText
            {
                Heading = new HeadingInfo("TaskHubStressTest", "1.01"),
                AdditionalNewLineAfterOption = true,
                AddDashesToOption = true
            };
            help.AddPreOptionsLine("Usage: TaskHubStressTest.exe -c");
            help.AddPreOptionsLine("Usage: TaskHubStressTest.exe -c -s <id>");
            help.AddPreOptionsLine("Usage: TaskHubStressTest.exe -i <id>");
            help.AddOptions(options);
            return help;
        }
#else
        [Option('c', "create-hub", DefaultValue = false,
            HelpText = "Create Orchestration Hub.")]
        public bool CreateHub { get; set; }

        [Option('s', "start-instance", DefaultValue = null,
            HelpText = "Start Driver Instance")]
        public string StartInstance { get; set; }

        [Option('i', "instance-id",
            HelpText = "Instance id for new orchestration instance.")]
        public string InstanceId { get; set; }

        [HelpOption]
        public string GetUsage()
        {
            // this without using CommandLine.Text
            //  or using HelpText.AutoBuild

            var help = new HelpText
            {
                Heading = new HeadingInfo("TaskHubStressTest", "1.0"),
                AdditionalNewLineAfterOption = true,
                AddDashesToOption = true
            };
            help.AddPreOptionsLine("Usage: TaskHubStressTest.exe -c");
            help.AddPreOptionsLine("Usage: TaskHubStressTest.exe -c -s <id>");
            help.AddPreOptionsLine("Usage: TaskHubStressTest.exe -i <id>");
            help.AddOptions(this);
            return help;
        }
#endif
    }
}
