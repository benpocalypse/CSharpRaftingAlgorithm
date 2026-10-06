namespace SharedCode
{
	public class RaftAPI
	{
		public enum MessageType
		{
			AppendEntries = 0,
			RequestVote = 1,
			HeartBeat = 2, // Does dual purpose in Raft. It might be empty (ping) or it might contain log history, if a follower is lagging.
			AppendEntriesSuccess = 3,
			AppendEntriesFailure = 4,
			RequestVoteResponse = 5
		}

		public delegate void SendMessage(int sourceNodeNumber, int targetNodeNumber, RaftMessage msg);
		public delegate void SendRequestVoteMessage(int candidateTerm, int candidateId, int lastLogIndex, int lastLogTerm);

		public class RaftMessage
		{
			public int SourceNodeNumber = 0;
			public MessageType Type = MessageType.HeartBeat;
			public string Message = string.Empty;

			public RaftMessage()
			{

			}

			public RaftMessage(RaftMessage other)
			{
				SourceNodeNumber = other.SourceNodeNumber;
				Type = other.Type;
				Message = other.Message;
			}

			public RaftMessage(MessageType type, int sourceNode, string data)
			{
				Type = type;
				SourceNodeNumber = sourceNode;
				Message = data;
			}

			public RaftMessage(string encodedString)
			{
				var parts = encodedString.Split(',');
				SourceNodeNumber = int.Parse(parts[0]);
				Type = (MessageType)int.Parse(parts[1]);
				Message = parts[2];
			}

			public override string ToString()
			{
				return SourceNodeNumber.ToString() + "," + (int)Type + "," + Message;
			}
		}
	}
}
