using System;
using System.Collections.Generic;
using System.Linq;

namespace SharedCode
{
	public enum FollowerState
	{
		Unknown,
		NeedsUpdate,
		UpToDate
	}

	public enum FollowerResponse
	{
		Failure,
		Success
	}

	public class Follower
	{
		public FollowerState State = FollowerState.Unknown;
		public int NodeNumber = 0;

		public Follower(int nodeNum)
		{
			NodeNumber = nodeNum;
		}
	}
	
	public class LeaderLogic
	{
		// FIXME - For whatever reason, the shared code can't reference ImmutableLists. Beware thread bugs.
		//public ImmutableList<Follower> Followers = ImmutableList<Follower>.Empty;

		// List isn't thread safe.
		public List<Follower> Followers = new List<Follower>();
		public Dictionary<int, int> NextIndex = new Dictionary<int, int>();
		public Dictionary<int, int> MatchIndex = new Dictionary<int, int>();
		public int Term = 1;
		public int LeaderNodeNumber = 0;
		public int CommitIndex = 0;
		public int ConsensusNumber = 0;
		public bool ConsensusReached = false;

		public LeaderLogic(int leaderNodeNumber)
		{
			LogWriter.Write(LogWriter.Source.LEADERLOGIC, $"Leader Node is {leaderNodeNumber}.");

			LeaderNodeNumber = leaderNodeNumber;
			NextIndex.Add(leaderNodeNumber, 1);
		}

		public void UpdateIndicies(int nodeNum, int value)
		{
			LogWriter.Write(LogWriter.Source.LEADERLOGIC, $"Updating indicies for NextIndex at Node {nodeNum} to {value}.");
			NextIndex[nodeNum] = value;
		}

		public void AddFollower(int nodeNum)
		{
			LogWriter.Write(LogWriter.Source.LEADERLOGIC, $"Adding follower Node {nodeNum} to Node {LeaderNodeNumber}.");
			Followers.Add(new Follower(nodeNum));
			NextIndex.Add(nodeNum, 1);
			MatchIndex.Add(nodeNum, 0);
		}

		public FollowerState ProcessFollowerResponse(int nodeNum, FollowerResponse response)
		{
			var originalNode = Followers.Where(node => node.NodeNumber == nodeNum).FirstOrDefault();
			var updatedNode = originalNode;

			LogWriter.Write(LogWriter.Source.LEADERLOGIC, $"Processing follower response {response} from {nodeNum}.");

			if (response == FollowerResponse.Success)
			{
				if ((NextIndex[nodeNum]) == NextIndex[LeaderNodeNumber])
				{
					updatedNode.State = FollowerState.UpToDate;
				}
				else
				{
					updatedNode.State = FollowerState.NeedsUpdate;
					NextIndex[nodeNum] += 1;
				}

				MatchIndex[nodeNum] += 1;
			}
			else
			{
				updatedNode.State = FollowerState.NeedsUpdate;

				if (NextIndex[nodeNum] > 0)
				{
					NextIndex[nodeNum] -= 1;
				}
			}

			Followers.Remove(originalNode);
			Followers.Add(updatedNode);

			(ConsensusReached, ConsensusNumber) = GetConsensus();

			LogWriter.Write(LogWriter.Source.LEADERLOGIC, $"Updated indicies for NextIndex at Node {nodeNum} to {NextIndex[nodeNum]}.");
			LogWriter.Write(LogWriter.Source.LEADERLOGIC, $"Updated indicies for MatchIndex at Node {nodeNum} to {MatchIndex[nodeNum]}.");
			LogWriter.Write(LogWriter.Source.LEADERLOGIC, $"Consensus is {ConsensusReached} with value {ConsensusNumber}.");

			return updatedNode.State;
		}

		public (bool consensusReached, int consensusNumber) GetConsensus()
		{
			Dictionary<int, int> valCount = new Dictionary<int, int>();

			foreach (int i in MatchIndex.Values)
			{
				if (valCount.ContainsKey(i))
				{
					valCount[i]++;
				}
				else
				{
					valCount[i] = 1;
				}
			}

			var maxValue = valCount.Values.Max();

			if (maxValue > (Followers.Count / 2))
			{
				var maxEntry = valCount.Where(test => test.Value == maxValue).FirstOrDefault();
				return (true, maxEntry.Key);
			}
			else
			{
				return (false, 0);
			}
		}
	}
}
