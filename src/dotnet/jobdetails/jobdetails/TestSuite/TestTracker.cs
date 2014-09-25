/*
 * Created by SharpDevelop.
 * User: gomoz
 * Date: 17/02/2012
 * Time: 17:33
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;

using Microsoft.Hpc.Scheduler;
using Microsoft.Hpc.Scheduler.Properties;
using NUnit.Framework;

namespace Logscape.Microsoft.Hpc.TestSuite
{
	[TestFixture]
	public class TestTracker
	{
		[Test]
		public void TestPrevDiffCurr()
		{
			Grid grid = new Grid(null);
			HashSet<int> prev = new HashSet<int>() {1,2,3,4,6};
			HashSet<int> curr = new HashSet<int>() {3,4,5,6};
			HashSet<int> diff = new HashSet<int>(prev.Except(curr));			
			//HashSet<int> diff = grid.getSetDifference(prev,curr);
			HashSet<int> expected = new HashSet<int>(){1,2};


			
			Assert.AreEqual(expected.ToArray(), diff.ToArray());
			
			
		}
		
		[Test]
		public void TestCurrDiffPrev()
		{
			
			Grid grid = new Grid(null);		
			HashSet<int> prev = new HashSet<int>() {3,4,5,6};
			HashSet<int> curr = new HashSet<int>() {1,2,3,4,5,6};
			HashSet<int> diff = new HashSet<int>(prev.Except(curr));			
			//HashSet<int> diff = grid.getSetDifference(prev,curr);
			
			HashSet<int> expected = new HashSet<int>();
 
			Assert.AreEqual(expected.ToArray(), diff.ToArray());
			
		}
		
	}
}
