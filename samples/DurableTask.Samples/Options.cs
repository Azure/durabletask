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

namespace DurableTask.Samples
{
    using System.Collections.Generic;
    using CommandLine;
    using CommandLine.Text;

    internal class Options
    {
        [Option('c', "create-hub", Default = false,
            HelpText = "Create Orchestration Hub.")]
        public bool CreateHub { get; set; }

        [Option('s', "start-instance", Default = null,
            HelpText = "Start new instance.  Supported Orchestrations: 'Greetings, Cron, Average, ErrorHandling Signal'.")]
        public string StartInstance { get; set; }

        [Option('i', "instance-id",
            HelpText = "Instance id for new orchestration instance.")]
        public string InstanceId { get; set; }

        [Option('p', "params",
            HelpText = "Parameters for new instance.")]
        public IEnumerable<string> Parameters { get; set; }

        [Option('n', "signal-name",
            HelpText = "Instance id to send signal")]
        public string Signal { get; set; }

        [Option('w', "skip-worker", Default = false,
            HelpText = "Don't start worker")]
        public bool SkipWorker { get; set; }

        public static string GetUsage(ParserResult<Options> options)
        {
            // this without using CommandLine.Text
            //  or using HelpText.AutoBuild

            var help = new HelpText
            {
                Heading = new HeadingInfo("DurableTaskSamples", "1.0"),
                AdditionalNewLineAfterOption = true,
                AddDashesToOption = true
            };
            help.AddPreOptionsLine("Usage: DurableTaskSamples.exe -c -s Greetings");
            help.AddPreOptionsLine("Usage: DurableTaskSamples.exe -c -s Greetings2 -p 10");
            help.AddPreOptionsLine("Usage: DurableTaskSamples.exe -c -s Cron");
            help.AddPreOptionsLine("Usage: DurableTaskSamples.exe -c -s Cron -p \"0 12 * */2 Mon\"");
            help.AddPreOptionsLine("Usage: DurableTaskSamples.exe -c -s Average -p 1 50 10");
            help.AddPreOptionsLine("Usage: DurableTaskSamples.exe -c -s ErrorHandling");
            help.AddPreOptionsLine("Usage: DurableTaskSamples.exe -c -s SumOfSquares");
            help.AddPreOptionsLine("Usage: DurableTaskSamples.exe -c -s Signal -i 1");
            help.AddPreOptionsLine("Usage: DurableTaskSamples.exe -w -n User -i 1 -p MyName");
            help.AddOptions(options);
            return help;
        }
    }
}
