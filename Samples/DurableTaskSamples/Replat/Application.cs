namespace DurableTaskSamples.Replat
{
    public class Application
    {
        /// <summary>
        /// The name of the application in the management database
        /// </summary>
        public string Name { get; set; }

        public string Region { get; set; }

        /// <summary>
        /// The name of the site in Antares. Typically this is the same as <see cref="Name"/> but can be different in DR
        /// scenarios.
        /// </summary>
        public string SiteName { get; set; }

        /// <summary>
        /// The runtime platform of the application
        /// </summary>
        public RuntimePlatform Platform { get; set; }
    }

    public enum RuntimePlatform
    {
        /// <summary>
        /// Node.js platform. This is the default.
        /// </summary>
        Node = 0,

        /// <summary>
        /// .NET platform.
        /// </summary>
        DotNet = 1
    }
}
