namespace TaskHubStressTest
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
    }
}
