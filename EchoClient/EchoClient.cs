using System;
using SharedCode;

namespace EchoClient
{
	public class EchoClient
	{
		private static ThreadedUdpClient client1 = null;

		static void Main(string[] args)
		{
			/*
			var server2 = new ThreadedUdpClient(sendAddress: "127.0.0.1",
												sendPortNumber: 1231);

			client1 = new ThreadedUdpClient(listenAddress: "127.0.0.1",
												listenPortNumber: 1230,
												dataCallback: MessageReceived1);

			client1.Start();

			Console.WriteLine("Client Started...");

			while (true)
			{
				server2.Send(Console.ReadLine());
			}
			*/
		}

		public static void MessageReceived1(string msg)
		{
			Console.WriteLine($"EchoClient recieved msg: {msg}");
		}
	}
}
