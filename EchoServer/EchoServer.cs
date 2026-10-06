using System;
using System.Threading;
using SharedCode;

namespace EchoServer
{
	public class EchoServer
	{
		private static ThreadedUdpClient client1 = null;
		static void Main(string[] args)
		{
			/*
			server1 = new ThreadedUdpSender(sendAddress: "127.0.0.1",
											sendPortNumber: 1230);

			client1 = new ThreadedUdpReceiver(listenAddress: "127.0.0.1",
												listenPortNumber: 1231,
												dataCallback: MessageReceived1);

			client1.Start();
			Console.WriteLine("Server(s) Started...");

			while (true)
			{
			}
			*/
		}

		public static void MessageReceived1(string msg)
		{
			Console.WriteLine($"EchoServer1 recieved msg: {msg}");
			//server1.Send(msg);
			Console.WriteLine($"EchoServer1 replied with: {msg}");
		}
	}
}

