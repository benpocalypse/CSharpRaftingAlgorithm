using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using static SharedCode.RaftAPI;

namespace SharedCode
{
	public class Transport
	{
		private ThreadedUdpClient udpClient;

		public Transport(IPEndPoint address, int listenPort)
		{
			udpClient = new ThreadedUdpClient(address, listenPort, ReceiveStringMessage);
			udpClient.Start();
		}

		public void SendRaftMessage(RaftMessage msg)
		{
			udpClient.Send(msg.ToString());
		}

		public void ReceiveRaftMessage(RaftMessage msg)
		{

		}

		public void ReceiveStringMessage(string msg)
		{
			LogWriter.Write(LogWriter.Source.TRANSPORT, $"Transport received message {msg}");
			//return new RaftAPI.RaftMessage(msg);
		}

		public void SendStringMessage(string msg)
		{
			udpClient.Send(msg);
		}
	}
}
