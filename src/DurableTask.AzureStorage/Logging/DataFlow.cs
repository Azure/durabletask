namespace DurableTask.AzureStorage.Logging
{
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Extension methods for the <see cref="DataFlowLogger"/> class.
    /// </summary>

    public static class DataFlowLogger
    {
        private static readonly ILogger _logger;

        static DataFlowLogger()
        {
            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.AddConsole();
                builder.AddFile("Logs/{Date}.txt");
            });

            _logger = loggerFactory.CreateLogger("MyLogger");
        }

        /// <summary>
        /// Prints something onto a file.
        /// </summary>
        /// <param name="message">The message to print.</param>
        public static void LogInformation(string message)
        {
            _logger.LogInformation(message);
        }
    }

}