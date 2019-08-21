using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.SqlServer.Tests
{
    public class AppSettings
    {
        public Uri DockerEndpoint { get; set; }
        public ContainerInformation SqlContainer { get; set; }
        public IDictionary<string, string> ConnectionStrings { get; set; }
        
        public class ContainerInformation
        {
            public string Image { get; set; }
            public string Tag { get; set; }
        }
    }     
}
