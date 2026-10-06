using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyValueStore
{
	public sealed class KeyValueStore
	{
		private ConcurrentDictionary<string, string> DataStore = new ConcurrentDictionary<string, string>();

		static void Main(string[] args)
		{
		}
	}
}
