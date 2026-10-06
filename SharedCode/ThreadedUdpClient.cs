using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using static SharedCode.RaftAPI;

namespace SharedCode
{
	public delegate void StringReceiver(string msg);

	public class ThreadedUdpClient
	{
		// Receive from
		private static UdpClient udpServer = null;
		private static IPEndPoint listenEP = null;

		// Send to
		private static UdpClient udpClient = null;
		private static IPEndPoint clientEP = null;

		private static Thread connectThread = null;
		private bool threadRunning = true;
		public StringReceiver StringReceived = null;

		public ThreadedUdpClient(string sendAddress, int sendPortNumber, int listenPortNumber, StringReceiver dataCallback)
		{
			StringReceived = dataCallback;
			udpServer = new UdpClient(listenPortNumber);
			listenEP = new IPEndPoint(IPAddress.Any, listenPortNumber);

			udpClient = new UdpClient();
			clientEP = new IPEndPoint(IPAddress.Parse(sendAddress), sendPortNumber);
		}

		public ThreadedUdpClient(IPEndPoint sendAddress, int listenPortNumber, StringReceiver dataCallback)
		{
			StringReceived = dataCallback;
			udpServer = new UdpClient(sendAddress.Port);
			listenEP = new IPEndPoint(IPAddress.Any, listenPortNumber);

			udpClient = new UdpClient();
			clientEP = new IPEndPoint(sendAddress.Address, sendAddress.Port);
		}

		public void Send(string msg)
		{
			var bytes = Encoding.ASCII.GetBytes(msg);
			udpClient.Send(bytes, bytes.Length, clientEP);
		}

		public void StringMessageReceived(string msg)
		{
			StringReceived?.Invoke(msg);
		}


		public void Start()
		{
			connectThread = new Thread(() =>
			{
				while (threadRunning)
				{
					Thread.CurrentThread.IsBackground = true;
					var data = Encoding.ASCII.GetString(udpServer.Receive(ref listenEP));
					StringMessageReceived(data);
					Thread.Yield();
				}
			});

			connectThread.Start();
		}

		public void Stop()
		{
			Volatile.Write(ref threadRunning, false);
		}
	}
}
