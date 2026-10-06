using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Xml;
using System.Xml.Serialization;
using SharedCode;

namespace RaftNetwork
{
	public class LogEntry
	{
		public int Term { get; set; } = 0;
		public string Entry { get; set; } = string.Empty;

		public LogEntry()
		{

		}

		public LogEntry(int term, string contents)
		{
			Term = term;
			Entry = contents;
		}
	}

	public class RaftLog
	{
		public ImmutableList<LogEntry> Log = ImmutableList<LogEntry>.Empty;

		public RaftLog()
		{

		}

		public RaftLog(int term, string item)
		{
			Log = Log.Add(new LogEntry(term, item));
		}

		public RaftLog(ImmutableList<LogEntry> log)
		{
			Log = log;
		}

		public RaftLog(LogEntry entry)
		{
			Log = Log.Add(entry);
		}

		public bool AppendEntries(int prevLogIndex, int prevLogTerm, ImmutableList<LogEntry> entries)
		{
			LogWriter.Write(LogWriter.Source.LOG, $"Attempting to append entries at previous entry {prevLogIndex} with previous log term {prevLogTerm}.");

			// !!!!!! CAUTION !!!!!!! Raft paper uses 1-based indexing. 
			// The log is never allowed to have holes in it.  This fails.
			if (prevLogIndex > Log.Count())
			{
				LogWriter.Write(LogWriter.Source.LOG, $"Failed to add log entry.");
				return false;
			}

			if (prevLogIndex > 0 && prevLogIndex < Log.Count)
			{
				// The term number in the log must match the term number in prev_term.
				// A mismatch could occur if a log had entries from a previous leader.
				if (Log[prevLogIndex].Term != prevLogTerm)
				{
					LogWriter.Write(LogWriter.Source.LOG, $"Failed to add log entry.");
					return false;
				}
			}

			// If the log already contains an entry that's located at the insertion
			// point, and that entry is from an earlier term, then it and all entries
			// after it need to be deleted.  See Figure 2, "Receiver Implementation"
			// of "AppendEntriesRPC", item 3. 
			//if (entries.Count > 0 && prevIndex + 1 < Log.Count && Log[prevIndex + 1].Term < entries[0].Term)
			if (entries.Count > 1 && prevLogIndex < Log.Count && Log[prevLogIndex].Term <= entries[0].Term)
			{
				entries = entries.RemoveRange(0, prevLogIndex+1);
			}

			Log = Log.AddRange(entries);

			LogWriter.Write(LogWriter.Source.LOG, $"Successfully added log entry.");

			return true;
		}

		public int GetCurrentIndex()
		{
			return Log.Count - 1;
		}

		public int GetLatestTerm()
		{
			if (Log.Count > 0)
			{
				return Log[Log.Count - 1].Term;
			}
			else
			{
				return 0;
			}
		}

		public string ToCommaDelimitedList()
		{
			int count = 0;
			string result = string.Empty;

			if (Log.Count > 0)
			{
				foreach (LogEntry log in Log)
				{
					result = result + $"{log.Term},{log.Entry},";
					count++;
				}

				result = result.Remove(result.Length - 1, 1);
			}

			return result;
		}

		public string ToCommaDelimitedListWithoutIndicies()
		{
			int count = 0;
			string result = string.Empty;

			if (Log.Count > 0)
			{
				foreach (LogEntry log in Log)
				{
					result = result + $"{log.Entry},";
					count++;
				}

				result = result.Remove(result.Length - 1, 1);
			}


			return result;
		}
	}
}
