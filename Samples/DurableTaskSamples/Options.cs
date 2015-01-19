namespace DurableTaskSamples
{
    using CommandLine;
    using CommandLine.Text;
    using System.Text;

    class Options
    {
        [Option('c', "create-hub", DefaultValue = false,
            HelpText = "Create Orchestration Hub.")]
        public bool CreateHub { get; set; }

        [Option('s', "start-instance", DefaultValue = null,
            HelpText = "Start new instance.  Suported Orchestrations: 'Greetings, Cron, Average, ErrorHandling Signal'.")]
        public string StartInstance { get; set; }

        [Option('i', "instance-id",
            HelpText = "Instance id for new orchestration instance.")]
        public string InstanceId { get; set; }

        [OptionArray('p', "params",
            HelpText = "Parameters for new instance.")]
        public string[] Parameters { get; set; }

        [Option('n', "signal-name",
            HelpText = "Instance id to send signal")]
        public string Signal { get; set; }

        [Option('w', "skip-worker", DefaultValue = false,
            HelpText = "Don't start worker")]
        public bool SkipWorker { get; set; }

        [HelpOption]
        public string GetUsage()
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
            help.AddPreOptionsLine("Usage: DurableTaskSamples.exe -c -s Signal -i 1");
            help.AddPreOptionsLine("Usage: DurableTaskSamples.exe -w -n User -i 1 -p MyName");
            help.AddOptions(this);
            return help;
        }
    }
}
