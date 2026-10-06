using System;
using System.Globalization;
using System.IO;
using System.Reflection;

namespace SharedCode
{
	public sealed class LogWriter
	{
		public enum Source
		{
			TRANSPORT,
			NETWORK,
			LOG,
			NODE,
			NODELOGIC,
			LEADERLOGIC
		}

		private static readonly object locker = new object();
		private static ulong seq = 0;

		public static void Write(Source source, string logMessage)
		{
			var m_exePath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
			var m_exeName = System.Diagnostics.Process.GetCurrentProcess().ProcessName;

			lock (locker)
			{
				seq += 1;
				using (StreamWriter w = File.AppendText(m_exePath + "\\" + m_exeName + "_log.txt"))
				{
					Log(seq + " " + source.ToString() + " " + logMessage, w);
				}
			}
		}

		private static void Log(string logMessage, TextWriter txtWriter)
		{
				txtWriter.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture)} {logMessage}");
		}
	}
}
