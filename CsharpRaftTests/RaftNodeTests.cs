using RaftNetwork;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Immutable;
using FluentAssertions;
using System.Net;
using static SharedCode.RaftAPI;

namespace CsharpRaftTests
{
	[TestClass]
	public class RaftNodeTests
	{
		[TestMethod]
		public void SequentialAppendInSingleLog()
		{
			// FIXME - test this 
			//RaftNode node = new RaftNode(0, new IPEndPoint(IPAddress.Parse("127.0.0.1"), 1234), SendMessageToNode, true, NetworkBackend.Local);
		}

		private void SendMessageToNode(int sourceNodeNumber, int targetNodeNumber, RaftMessage msg)
		{
			//nodeList.Where(node => node.NodeNumber == targetNodeNumber).FirstOrDefault().ReceiveMessageFromLocalNetwork(msg);
		}
	}
}
