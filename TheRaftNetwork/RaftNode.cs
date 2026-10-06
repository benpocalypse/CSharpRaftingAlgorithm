using System;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Linq;
using System.Net;
using System.Threading;
using SharedCode;
using static RaftNetwork.RaftNetwork;
using static SharedCode.RaftAPI;
using static SharedCode.Transport;

namespace RaftNetwork
{
	public class RaftNode
	{
		public int NodeNumber = 0;
		public IPEndPoint NodeAddress = null;
		private ImmutableList<(int NodeNumber, IPEndPoint NodeAddress)> NeighborList = ImmutableList<(int NodeNumber, IPEndPoint NodeAddress)>.Empty;
		private ConcurrentQueue<RaftMessage> Inbox = new ConcurrentQueue<RaftMessage>();
		private Transport transportLayer = null;

		private NetworkBackend networkBackend = NetworkBackend.OnTheWire;
		private SendMessage sendMessageToNode;
		//private RaftMessageReceiver ReceiveMessageFromNode;
		private bool disabled = false;

		private static Thread receiveThread = null;
		private bool receiveThreadRunning = true;

		private static Thread logicThread = null;
		private bool logicThreadRunning = true;
		private bool justElectedLeader = false;

		public LeaderLogic LeaderLogic = null;
		private NodeLogic nodeLogic = null;

		// FIXME - make this private
		public RaftLog ServerLog = new RaftLog();

		public RaftNode(int number, IPEndPoint address, SendMessage sendCallback, NetworkBackend backend = NetworkBackend.OnTheWire)
		{
			NodeNumber = number;
			NodeAddress = address;
			sendMessageToNode = sendCallback;
			networkBackend = backend;

			nodeLogic = new NodeLogic(NodeNumber, SendRequestVoteMessageToNode);

			if (networkBackend == NetworkBackend.Local)
			{
				//SendMessageToNode = sendCallback;
			}
			else
			{
				//transportLayer = new Transport(address, address.Port + 1, ReceiveMessageFromNode);
			}

			logicThread = new Thread(() =>
			{
				while (logicThreadRunning)
				{
					nodeLogic.Process();

					if (LeaderLogic != null)
					{
						if (justElectedLeader == true)
						{
							justElectedLeader = false;
							KickoffLogReplication();
						}
					}

					Thread.Sleep(1);
				}
			});

			logicThread.Start();

			receiveThread = new Thread(() =>
			{
				while (receiveThreadRunning)
				{
					if (!Inbox.IsEmpty)
					{
						RaftMessage msg;

						while (!Inbox.TryDequeue(out msg))
						{
							Thread.Sleep(1);
						}

						if (msg.Type == MessageType.AppendEntries)
						{
							// FIXME - refactor this hacky string splitting nonsense and replace it with proper serialization/deserialization.
							// UPDATE - It turns out that Immutable collections aren't serializable. And you can't short circuit this behavior. Yay.
							var parts = msg.Message.Split(',');

							var entries = ImmutableList<LogEntry>.Empty;

							nodeLogic.LeaderId = int.Parse(parts[0]);

							for (int i = 4; i < parts.Length-1; i+= 2)
							{
								entries = entries.Add(new LogEntry(int.Parse(parts[i]), parts[i + 1]));
							}

							if (ServerLog.AppendEntries(int.Parse(parts[2]), int.Parse(parts[3]), entries) == true)
							{
								DisplayLog();
								
								nodeLogic.CommitIndex += 1;
								nodeLogic.LastLogIndex += 1;
								nodeLogic.LastLogTerm = int.Parse(parts[3]);

								if (ServerLog.GetLatestTerm() > nodeLogic.CurrentTerm)
								{
									nodeLogic.CurrentTerm = ServerLog.GetLatestTerm();
									nodeLogic.HasVotedInCurrentElection = false;
								}

								LogWriter.Write(LogWriter.Source.NODE, $"Node {NodeNumber} added a log entry.");
								SendMessage(msg.SourceNodeNumber, new RaftMessage(MessageType.AppendEntriesSuccess, NodeNumber, $"{nodeLogic.CurrentTerm}"));
							}
							else
							{
								LogWriter.Write(LogWriter.Source.NODE, $"Node {NodeNumber} did NOT add a log entry.");
								SendMessage(msg.SourceNodeNumber, new RaftMessage(MessageType.AppendEntriesFailure, NodeNumber, string.Empty));
							}
						}

						// For these 2 message types, in theory only a leader would receive these, so we can skip that check here.
						if (msg.Type ==  MessageType.AppendEntriesSuccess)
						{
							LogWriter.Write(LogWriter.Source.NODE, $"Leader recieved a success response from Node {msg.SourceNodeNumber} with term {nodeLogic.CurrentTerm}.");

							if ( LeaderLogic.ProcessFollowerResponse(msg.SourceNodeNumber, FollowerResponse.Success) == FollowerState.NeedsUpdate )
							{
								RaftLog newLog = new RaftLog(ServerLog.Log.GetRange(0, LeaderLogic.NextIndex[msg.SourceNodeNumber]));

								var prevLogIndex = LeaderLogic.NextIndex[msg.SourceNodeNumber] - 2;
								var prevLogTerm = ServerLog.Log[prevLogIndex].Term;

								var responseMsg = new RaftMessage(
										MessageType.AppendEntries,
										NodeNumber,
										$"{nodeLogic.LeaderTerm},{nodeLogic.LeaderId},{prevLogIndex},{prevLogTerm},{newLog.ToCommaDelimitedList()},{LeaderLogic.CommitIndex}"
									);

								SendMessage(msg.SourceNodeNumber, responseMsg);
							}
							else
							{
								LogWriter.Write(LogWriter.Source.NODE, $"Node {msg.SourceNodeNumber} is up to date!");
								LeaderLogic.GetConsensus();
							}
						}

						if (msg.Type == MessageType.AppendEntriesFailure)
						{
							LogWriter.Write(LogWriter.Source.NODE, $"Leader recieved a failure response from Node {msg.SourceNodeNumber}.");
							LeaderLogic.ProcessFollowerResponse(msg.SourceNodeNumber, FollowerResponse.Failure);

							RaftLog newLog = new RaftLog(ServerLog.Log.GetRange(0, LeaderLogic.NextIndex[msg.SourceNodeNumber]));
							if (!newLog.Log.IsEmpty)
							{

								if (LeaderLogic.NextIndex[msg.SourceNodeNumber] >= 2)
								{
									var prevLogIndex = LeaderLogic.NextIndex[msg.SourceNodeNumber] - 2;
									var prevLogTerm = ServerLog.Log[prevLogIndex].Term;

									var responseMsg = new RaftMessage(
											MessageType.AppendEntries,
											NodeNumber,
											$"{nodeLogic.LeaderTerm},{nodeLogic.LeaderId},{prevLogIndex},{prevLogTerm},{newLog.ToCommaDelimitedList()},{nodeLogic.CommitIndex}"
										);

									SendMessage(msg.SourceNodeNumber, responseMsg);
								}
							}
						}

						if (msg.Type == MessageType.RequestVote)
						{
							var parts = msg.Message.Split(',');

							var votedFor = false;
							var requestTerm = int.Parse(parts[0]);
							var requestId = int.Parse(parts[1]);
							var requestLastLogIndex = int.Parse(parts[2]);
							var requestLastLogTerm = int.Parse(parts[3]);

							if ( (requestTerm < nodeLogic.CurrentTerm) )
							{
								votedFor = false;
							}
							else if ( !(nodeLogic.HasVotedInCurrentElection) && (requestLastLogTerm >= ServerLog.GetLatestTerm()) )
							{
								nodeLogic.HasVotedInCurrentElection = true;
								votedFor = true;
							}

							LogWriter.Write(LogWriter.Source.NODE, $"Node {NodeNumber} got a vote request, sending vote of {votedFor} to Node {msg.SourceNodeNumber}.");
							SendMessage(msg.SourceNodeNumber, new RaftMessage(MessageType.RequestVoteResponse, NodeNumber, $"{nodeLogic.CurrentTerm},{votedFor}"));
						}

						if (msg.Type == MessageType.RequestVoteResponse && !(nodeLogic.IsLeader) )
						{
							var parts = msg.Message.Split(',');
							var termResult = int.Parse(parts[0]);
							var voteResult = bool.Parse(parts[1]);

							LogWriter.Write(LogWriter.Source.NODE, $"Node {NodeNumber} got a vote response of {voteResult} for term {termResult}.");
							nodeLogic.AddVote();

							if (((float)nodeLogic.VotesReceieved) > (((float)NeighborList.Count + 1) / 2.0f))
							{
								nodeLogic.WonElection();
								LeaderLogic = new LeaderLogic(NodeNumber);

								LeaderLogic.UpdateIndicies(NodeNumber, ServerLog.GetCurrentIndex() + 1);

								foreach (var node in NeighborList)
								{
									LeaderLogic.AddFollower(node.NodeNumber);
									LeaderLogic.UpdateIndicies(node.NodeNumber, ServerLog.GetCurrentIndex() + 1);
								}

								justElectedLeader = true;
							}
						}
					}

					Thread.Sleep(1);
				}
			});

			receiveThread.Start();
		}

		public bool IsLeader()
		{
			return nodeLogic.IsLeader;
		}

		public void KickoffLogReplication()
		{
			LogWriter.Write(LogWriter.Source.NODE, $"Leader node is kicking off log replication.");
			if (ServerLog.Log.Count > 0)
			{
				var prevLogIndex = ServerLog.Log.Count - 2;
				var prevLogTerm = ServerLog.Log[ServerLog.Log.Count - 1].Term;
				nodeLogic.CommitIndex = ServerLog.Log.Count - 1;

				var msg = new RaftMessage(
						MessageType.AppendEntries,
						NodeNumber,
						$"{nodeLogic.LeaderTerm},{nodeLogic.LeaderId},{prevLogIndex},{prevLogTerm},{ServerLog.ToCommaDelimitedList()},{nodeLogic.CommitIndex}"
					);

				foreach (var node in NeighborList)
				{
					SendMessage(node.NodeNumber, msg);
				}
			}
		}

		public void KickoffLogReplication(LogEntry entry)
		{
			LogWriter.Write(LogWriter.Source.NODE, $"Leader node is kicking off log replication due to client request.");

			var prevLogIndex = ServerLog.Log.Count > 1 ? ServerLog.Log.Count - 2 : 0;
			var prevLogTerm = ServerLog.Log.Count > 1 ? ServerLog.Log[ServerLog.Log.Count - 1].Term : 0;
			nodeLogic.CommitIndex = ServerLog.Log.Count > 1 ? ServerLog.Log.Count - 1 : 0;

			if (ServerLog.AppendEntries(prevLogIndex, prevLogTerm, ImmutableList<LogEntry>.Empty.Add(entry)))
			{
				var msg = new RaftMessage(
						MessageType.AppendEntries,
						NodeNumber,
						$"{nodeLogic.LeaderTerm},{nodeLogic.LeaderId},{prevLogIndex},{prevLogTerm},{ServerLog.ToCommaDelimitedList()},{nodeLogic.CommitIndex}"
					);

				foreach (var node in NeighborList)
				{
					SendMessage(node.NodeNumber, msg);
				}
			}
		}

		public void UpdateLeaderLogic()
		{
			LeaderLogic.UpdateIndicies(NodeNumber, ServerLog.GetCurrentIndex() + 1);

			foreach (var node in NeighborList)
			{
				LeaderLogic.UpdateIndicies(node.NodeNumber, ServerLog.GetCurrentIndex() + 1);
			}
		}

		public void ReceiveMessageFromLocalNetwork(RaftMessage msg)
		{
			if (!disabled)
			{
				LogWriter.Write(LogWriter.Source.NODE, $"Node {NodeNumber} received message from Node {msg.SourceNodeNumber} {msg.Type} {msg.Message}");
				Inbox.Enqueue(msg);
			}
		}

		public void ReceiveMessageFromNode(RaftMessage msg)
		{
			LogWriter.Write(LogWriter.Source.NODE, $"Node {NodeNumber} received message from Node {msg.SourceNodeNumber} {msg.Type} {msg.Message}");
		}

		private void SendRequestVoteMessageToNode(int candidateTerm, int candidateId, int lastLogIndex, int lastLogTerm)
		{
			LogWriter.Write(LogWriter.Source.NODE, $"Node {NodeNumber} is sending a request vote message.");
			foreach (var node in NeighborList)
			{
				SendMessage(node.NodeNumber, new RaftMessage(MessageType.RequestVote, NodeNumber, $"{candidateTerm},{candidateId},{lastLogIndex},{lastLogTerm}"));
			}
		}

		public void SendMessage(int targetNodeNumber, RaftMessage msg)
		{
			if (networkBackend == NetworkBackend.Local)
			{
				sendMessageToNode?.Invoke(NodeNumber, targetNodeNumber, msg);
			}
			else
			{
				//transportLayer.SendRaftMessage(msg);
			}
		}

		public void AddNeighbor(int nodeNumber, IPEndPoint address, int listenPort)
		{
			var neighbor = (NodeNumber: nodeNumber, NodeAddres: address);

			NeighborList = NeighborList.Add(neighbor);
		}

		public void RemoveNeighbor(int nodeNumber)
		{
			NeighborList = NeighborList.Remove(NeighborList.Where(node => node.NodeNumber == nodeNumber).FirstOrDefault());
		}

		public void Disable()
		{
			disabled = true;
		}

		public void Enable()
		{
			disabled = false;
		}

		public void DisplayLog()
		{
			LogWriter.Write(LogWriter.Source.NODE, $"Displaying log contents for Node {NodeNumber} at Address {NodeAddress}");
			LogWriter.Write(LogWriter.Source.NODE, ServerLog.ToCommaDelimitedList());
		}
	}
}
