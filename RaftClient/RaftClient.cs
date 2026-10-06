using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace RaftClient
{
	public sealed class RaftClient
	{
		private static UdpClient udpServer = null;
		private static UdpClient updClient = null;
		private static IPEndPoint clientEP = null;
		private static Thread receiveThread = null;
		private static bool receiveThreadRunning = true;
		//private static ConcurrentQueue<string> Inbox = new ConcurrentQueue<string>();

		static void Main(string[] args)
		{
			int listenPortNum = 0;
			int sendPortNum = 0;

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
								string msg = Encoding.ASCII.GetString(udpServer.Receive(ref remoteEP));

								Console.WriteLine($"{System.Environment.NewLine}Message receieved: {msg}");

								Thread.Sleep(1);
							}
						});

						receiveThread.Start();

						Console.WriteLine("Raft Client started.");
						DisplayHelp();

						while (true)
						{
							Console.Write("$: ");
							var rawInput = Console.ReadLine();
							var splitInput = rawInput.Split(' ');

							switch (splitInput[0].ToLowerInvariant())
							{
								case "set":
									Console.WriteLine("Sending Raft 'set' command.");
									updClient.Send(Encoding.ASCII.GetBytes(rawInput), rawInput.Length, clientEP);
									break;

								case "get":
									Console.WriteLine("Sending Raft 'get' command.");
									updClient.Send(Encoding.ASCII.GetBytes(rawInput), rawInput.Length, clientEP);
									break;

								case "help":
									DisplayHelp();
									break;

								default:
									Console.WriteLine("Please enter a valid Raft command or type 'help' to list commands.");
									break;
							}
						}
					}
					catch(Exception e)
					{
						Console.WriteLine($"Caught exception: {e}");
					}
				}
			}
		}

		private static void DisplayHelp()
		{
			Console.WriteLine("--------------------------------------------------------------------------------");
			Console.WriteLine("To add a log entry, type: set <logValue>");
			Console.WriteLine("To view the current log, type: get");
			Console.WriteLine("To exit, type: exit");
		}
	}
}
