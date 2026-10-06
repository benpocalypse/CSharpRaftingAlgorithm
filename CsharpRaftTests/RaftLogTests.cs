using System;
using RaftNetwork;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Immutable;
using FluentAssertions;

namespace CsharpRaftTests
{
	[TestClass]
	public class RaftLogTests
	{
		[TestMethod]
		public void SequentialAppendInSingleLog()
		{
			RaftLog log1 = new RaftLog();

			log1.AppendEntries(0, 0, ImmutableList<LogEntry>.Empty.Add(new LogEntry(1, "a")));
			log1.AppendEntries(0, 1, ImmutableList<LogEntry>.Empty.Add(new LogEntry(1, "b")));
			log1.AppendEntries(1, 1, ImmutableList<LogEntry>.Empty.Add(new LogEntry(1, "c")));

			log1.Log.Should().HaveCount(3, "3 AppendEntries() were performed");
		}

		[TestMethod]
		public void IncorrectPreviousIndex()
		{
			RaftLog log1 = new RaftLog();

			log1.AppendEntries(0, 0, ImmutableList<LogEntry>.Empty.Add(new LogEntry(1, "a")));
			log1.AppendEntries(0, 1, ImmutableList<LogEntry>.Empty.Add(new LogEntry(1, "b")));
			log1.AppendEntries(1, 1, ImmutableList<LogEntry>.Empty.Add(new LogEntry(1, "c")));
			log1.AppendEntries(4, 1, ImmutableList<LogEntry>.Empty.Add(new LogEntry(1, "d")));

			log1.Log.Should().HaveCount(3, "4 AppendEntries() were performed, but the last one contained the incorrect prevIndex");
		}

		[TestMethod]
		public void IncorrectPreviousTerm()
		{
			RaftLog log1 = new RaftLog();

			log1.AppendEntries(0, 0, ImmutableList<LogEntry>.Empty.Add(new LogEntry(1, "a")));
			log1.AppendEntries(0, 1, ImmutableList<LogEntry>.Empty.Add(new LogEntry(1, "b")));
			log1.AppendEntries(1, 1, ImmutableList<LogEntry>.Empty.Add(new LogEntry(1, "c")));
			log1.AppendEntries(2, 0, ImmutableList<LogEntry>.Empty.Add(new LogEntry(1, "d")));

			log1.Log.Should().HaveCount(3, "4 AppendEntries() were performed, but the last one contained the incorrect prevTerm");
		}
	}
}
