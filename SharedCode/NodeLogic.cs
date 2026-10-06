using System;
using System.Collections.Generic;
using System.Text;
using static SharedCode.RaftAPI;

namespace SharedCode
{
	public sealed class NodeLogic
	{
		// Term is only ever increased by candidates.
		// Receiving any mesage with a higher term than yours makes you a follower.
		// Discard ALL received messages with a lower term than yourself.
		// Followers listen for heartbeat messages on a slightly randomized timeout.
		// If no leader heartbeat after timeout, follower sends a request vote message.
		// A node only gets a single vote during any election (for dealing with split votes.)
		// Voting requires NV storage...I'm going to punt on this.
		// A node may not vote for a candidate node if it's log is not as up-to-date as the voting node.

		public enum NodeState
		{
			Unknown,
			Candidate,
			Leader,
			Follower
		}

		public enum RaftEvent
		{
			Startup,
			HeartbeatTimeout,
			ElectionTimeout,
			ReceiveVoteMajority,
			DiscoverNodeWithHigherTerm,
			DiscoverCurrentLeaderOrNewTerm,
		}

		private NodeState nextState = NodeState.Follower;
		private NodeState currentState = NodeState.Unknown;

		public readonly int HeartbeatInterval = 0;
		public int HearbeatReceivedTimer = 0;

		public readonly int ElectionInterval = 0;
		public int ElectionIntervalTimer = 0;

		public int CommitIndex = 0;
		public int CurrentTerm = 0;
		public int LeaderTerm = 0;
		public int LeaderId = 0;
		public bool IsLeader = false;

		public int LastLogIndex = 0;
		public int LastLogTerm = 0;

		private int nodeNumber = 0;
		private int numNeighbors = 0;
		public int VotesReceieved = 0;

		public bool HasVotedInCurrentElection = false;

		private SendRequestVoteMessage sendRequestVoteMessage;

		public NodeLogic(int num, SendRequestVoteMessage voteMsg)
		{
			Random rnd = new Random();

			HeartbeatInterval = 1000 + rnd.Next(-500, 500);
			HearbeatReceivedTimer = HeartbeatInterval;

			ElectionInterval = 1000 + rnd.Next(-500, 500);
			ElectionIntervalTimer = ElectionInterval;

			/*
			if (num == 0)
			{
				HeartbeatInterval = 300;
				HearbeatReceivedTimer = HeartbeatInterval;

				ElectionInterval = 300;
				ElectionIntervalTimer = ElectionInterval;
			}
			else
			{
				HeartbeatInterval = 1000 + rnd.Next(-500, 500);
				HearbeatReceivedTimer = HeartbeatInterval;

				ElectionInterval = 1000 + rnd.Next(-500, 500);
				ElectionIntervalTimer = ElectionInterval;
			}
			*/

			nodeNumber = num;

			sendRequestVoteMessage = voteMsg;

			LogWriter.Write(LogWriter.Source.NODELOGIC, $"Starting Node Logic for Node {nodeNumber} with Heartbeat Interval of {HeartbeatInterval}, and Election Interval of {ElectionInterval}.");
		}

		private void ModeEntry(NodeState state)
		{
			LogWriter.Write(LogWriter.Source.NODELOGIC, $"Node {nodeNumber} is entering state {state}.");

			switch (state)
			{
				case NodeState.Unknown:
					break;

				case NodeState.Candidate:
					ElectionIntervalTimer = ElectionInterval;
					CurrentTerm++;
					VotesReceieved++;

					LogWriter.Write(LogWriter.Source.NODELOGIC, $"Node {nodeNumber} is a Candidate with Term {CurrentTerm} and is sending a vote request.");
					sendRequestVoteMessage(CurrentTerm, nodeNumber, LastLogIndex, LastLogTerm);
					break;

				case NodeState.Follower:
					HearbeatReceivedTimer = HeartbeatInterval;
					break;

				case NodeState.Leader:
					IsLeader = true;
					break;
			}
		}

		private void ModeExit(NodeState state)
		{
			LogWriter.Write(LogWriter.Source.NODELOGIC, $"Node {nodeNumber} is exiting state {state}.");

			switch (state)
			{
				case NodeState.Unknown:
					break;

				case NodeState.Candidate:
					break;

				case NodeState.Follower:
					break;

				case NodeState.Leader:
					IsLeader = false;
					break;
			}
		}

		public void Process()
		{
			if (nextState != currentState)
			{
				ModeExit(currentState);
				ModeEntry(nextState);

				currentState = nextState;
			}

			//LogWriter.Write(LogWriter.Source.NODELOGIC, $"Node {nodeNumber} is processing state {currentState}.");

			switch (currentState)
			{
				case NodeState.Unknown:
					break;

				case NodeState.Candidate:
					ElectionIntervalTimer--;

					if (ElectionIntervalTimer <= 0)
					{
						HandleEvent(RaftEvent.ElectionTimeout);
					}
					break;

				case NodeState.Follower:
					HearbeatReceivedTimer--;

					if (HearbeatReceivedTimer == 0)
					{
						HandleEvent(RaftEvent.HeartbeatTimeout);
					}
					break;

				case NodeState.Leader:
					break;
			}
		}

		public void HeartbeatReceieved()
		{
			LogWriter.Write(LogWriter.Source.NODELOGIC, $"Node {nodeNumber} received a heartbeat.");
			HearbeatReceivedTimer = HeartbeatInterval;
		}

		public void HandleEvent(RaftEvent raftEvent)
		{
			LogWriter.Write(LogWriter.Source.NODELOGIC, $"Node {nodeNumber} is handling event {raftEvent}");
			switch (raftEvent)
			{
				case RaftEvent.Startup:
					nextState = NodeState.Follower;
					break;

				case RaftEvent.HeartbeatTimeout:
					nextState = NodeState.Candidate;
					HearbeatReceivedTimer = HeartbeatInterval;
					break;

				case RaftEvent.ElectionTimeout:
					// TODO - This might not work as expected.
					//        The point is that we're a candidate, and we have to become a candidate again,
					//        and do all the candidate one time events.
					ModeEntry(NodeState.Candidate);
					break;

				case RaftEvent.ReceiveVoteMajority:
					nextState = NodeState.Leader;
					break;

				case RaftEvent.DiscoverNodeWithHigherTerm:
					nextState = NodeState.Follower;
					break;

				case RaftEvent.DiscoverCurrentLeaderOrNewTerm:
					nextState = NodeState.Follower;
					break;
			}
		}

		public void AddNeighbor()
		{
			LogWriter.Write(LogWriter.Source.NODELOGIC, $"Node {nodeNumber} is adding a neighbor.");
			numNeighbors++;
		}

		public void RemoveNeighbor()
		{
			LogWriter.Write(LogWriter.Source.NODELOGIC, $"Node {nodeNumber} is removing a neighbor.");
			if (numNeighbors > 0 )
			{
				numNeighbors--;
			}
		}

		public void AddVote()
		{
			VotesReceieved++;

			LogWriter.Write(LogWriter.Source.NODELOGIC, $"Node {nodeNumber} received a vote, for a total of {VotesReceieved}.");

			if (((float)VotesReceieved) > (((float)numNeighbors + 1) / 2.0f))
			{
				LogWriter.Write(LogWriter.Source.NODELOGIC, $"Node {nodeNumber} received a majority of the votes.");
				nextState = NodeState.Leader;
			}
		}

		public void ResetVotes()
		{
			LogWriter.Write(LogWriter.Source.NODELOGIC, $"Clearing received votes for {nodeNumber}.");
			VotesReceieved = 0;
		}

		public void WonElection()
		{
			LogWriter.Write(LogWriter.Source.NODELOGIC, $"Node {nodeNumber} won the election!.");
			nextState = NodeState.Leader;
		}
	}
}
