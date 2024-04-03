using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DurableTask.AzureStorage.ControlQueueHeartbeat
{
    /// <summary>
    /// 
    /// </summary>
    public class FileWriter
    {
        static string timeTicks = DateTime.UtcNow.Ticks.ToString();
        static string path = @$"C:\DTF\v2.16.1\Logs_{timeTicks}.txt";
        static string pathControlQueueMonitor = @$"C:\DTF\v2.16.1\Logs_ControlQueueMonitor_{timeTicks}.txt";
        static string pathControlQueueOrch = @$"C:\DTF\v2.16.1\Logs_ControlQueueOrch_{timeTicks}.txt";
        static string pathControlQueueProgram = @$"C:\DTF\v2.16.1\Logs_ControlQueueProgram_{timeTicks}.txt";

        static object obj = new object();

        private static void WriteLog(string path, string msg)
        {
            var modMsg = $"[{DateTime.UtcNow.ToLongTimeString()}] : {msg}" + Environment.NewLine;
            Console.WriteLine(modMsg);

            lock (obj)
            {
                File.AppendAllText(path, modMsg);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="msg"></param>
        public static void WriteLogControlQueueMonitor(string msg)
        {
            WriteLog(pathControlQueueMonitor, msg);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="msg"></param>
        public static void WriteLogControlQueueOrch(string msg)
        {
            WriteLog(pathControlQueueOrch, msg);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="msg"></param>
        public static void WriteLogControlQueueProgram(string msg)
        {
            WriteLog(pathControlQueueProgram, msg);
        }
    }
}
