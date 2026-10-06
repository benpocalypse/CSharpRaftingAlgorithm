using System;
using System.Collections.Immutable;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using SharedCode;
using static SharedCode.RaftAPI;

namespace RaftNetwork
{
	public enum NetworkBackend
	{
		Local,
		OnTheWire
	}

	public class RaftNetwork
	{
		public ImmutableList<RaftNode> nodeList = ImmutableList<RaftNode>.Empty;
		private static NetworkBackend backend = NetworkBackend.Local;

		private static UdpClient udpServer = null;
		private static UdpClient updClient = null;
		private static IPEndPoint clientEP = null;
		private static Thread receiveThread = null;
		private static bool receiveThreadRunning = true;

		public RaftNetwork()
		{
			// Create the nodes.
			nodeList = nodeList.Add(new RaftNode(0, new IPEndPoint(IPAddress.Parse("127.0.0.1"), 12340), SendMessageToNode, backend));
			nodeList = nodeList.Add(new RaftNode(1, new IPEndPoint(IPAddress.Parse("127.0.0.1"), 12350), SendMessageToNode, backend));
			nodeList = nodeList.Add(new RaftNode(2, new IPEndPoint(IPAddress.Parse("127.0.0.1"), 12360), SendMessageToNode, backend));
			nodeList = nodeList.Add(new RaftNode(3, new IPEndPoint(IPAddress.Parse("127.0.0.1"), 12370), SendMessageToNode, backend));
			nodeList = nodeList.Add(new RaftNode(4, new IPEndPoint(IPAddress.Parse("127.0.0.1"), 12380), SendMessageToNode, backend));

			// Wire up the network neighbors.
			foreach (RaftNode node in nodeList)
			{
				var neighborList = nodeList.Except(ImmutableList<RaftNode>.Empty.Add(node));

				foreach (RaftNode neighbor in neighborList)
				{
					node.AddNeighbor(neighbor.NodeNumber, neighbor.NodeAddress, neighbor.NodeAddress.Port);
				}
			}
		}

		private void SendMessageToNode(int sourceNodeNumber, int targetNodeNumber, RaftMessage msg)
		{
			LogWriter.Write(LogWriter.Source.NETWORK, $"Raft network sending message from Node {sourceNodeNumber} to Node {targetNodeNumber}.");

			nodeList.Where(node => node.NodeNumber == targetNodeNumber).FirstOrDefault().ReceiveMessageFromLocalNetwork(msg);
		}

		static void Main(string[] args)
		{
			LogWriter.Write(LogWriter.Source.NETWORK, $"Starting the {backend} Raft Network.");

			int listenPortNum = 0;
			int sendPortNum = 0;

			var network = new RaftNetwork();

			/*
			network.nodeList[0].ServerLog = new RaftLog();
			network.nodeList[0].ServerLog.AppendEntries(0, 1, ImmutableList<LogEntry>.Empty.Add(new LogEntry(1, "a")));
			network.nodeList[0].ServerLog.AppendEntries(0, 1, ImmutableList<LogEntry>.Empty.Add(new LogEntry(1, "b")));
			network.nodeList[0].ServerLog.AppendEntries(1, 1, ImmutableList<LogEntry>.Empty.Add(new LogEntry(1, "c")));
			network.nodeList[0].ServerLog.AppendEntries(2, 1, ImmutableList<LogEntry>.Empty.Add(new LogEntry(1, "d")));
			network.nodeList[0].ServerLog.AppendEntries(3, 1, ImmutableList<LogEntry>.Empty.Add(new LogEntry(1, "e")));
			network.nodeList[0].ServerLog.AppendEntries(4, 1, ImmutableList<LogEntry>.Empty.Add(new LogEntry(1, "f")));
			*/

			LogWriter.Write(LogWriter.Source.NETWORK, $"Leader log contents are {network.nodeList[0].ServerLog.ToCommaDelimitedList()}");

			Console.WriteLine($"Starting the {backend} Raft Network.");

			if ((args.Length == 0) || (args.Length < 1))
			{
				Console.WriteLine("Incorrect arguments specified. Please call me with the following syntax:");
				Console.WriteLine("  RaftClient.exe <portToListenOn> <portToSendOn>");
			}
			else
			{
				if ((int.TryParse(args[0], out listenPortNum)) && (int.TryParse(args[1], out sendPortNum)))
				{
					try
					{
						udpServer = new UdpClient(listenPortNum);
						var remoteEP = new IPEndPoint(IPAddress.Any, listenPortNum);

						updClient = new UdpClient();
						clientEP = new IPEndPoint(IPAddress.Parse("127.0.0.1"), sendPortNum);

						receiveThread = new Thread(() =>
						{
							while (receiveThreadRunning)
							{
								string recvMsg = Encoding.ASCII.GetString(udpServer.Receive(ref remoteEP));

								Console.WriteLine($"Raft Client message receieved: {recvMsg}");

								var splitInput = recvMsg.Split(' ');
								var leaderNode = network.nodeList.Where(node => node.IsLeader()).First();

								switch (splitInput[0])
								{
									case "set":
										leaderNode.KickoffLogReplication(new LogEntry(leaderNode.LeaderLogic.Term, splitInput[1]));
										break;

									case "get":
										var log = leaderNode.ServerLog.ToCommaDelimitedListWithoutIndicies();
										updClient.Send(Encoding.ASCII.GetBytes(log), log.Length, clientEP);
										break;
								}

								Thread.Sleep(1);
							}
						});

						receiveThread.Start();
					}
					catch (Exception e)
					{
						Console.WriteLine($"Caught exception: {e}");
					}
				}
			}

			Console.ReadLine();
		}
	}
}
