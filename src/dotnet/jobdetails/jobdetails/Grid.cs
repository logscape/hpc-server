using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Microsoft.Hpc.Scheduler;
using Microsoft.Hpc.Scheduler.Properties;

namespace Logscape.Microsoft.Hpc
{
    class Grid
    {

        IScheduler hpcScheduler;
        public ISchedulerCollection lastGetJobsRun;
        DateTime lastMinStartTime;
        DateTime tempMinStartTime;

        public Grid( IScheduler scheduler)
        {
            hpcScheduler = scheduler;
            lastMinStartTime = new DateTime(1970, 1, 1);
            tempMinStartTime = new DateTime(3000, 1, 1);
        }


        public HashSet<int> getSetDifference( HashSet<int> A, HashSet<int> B )
        {
        
        	List<int> ListA = new List<int>( A.ToArray() );
        	List<int> ListB = new List<int>( B.ToArray() );
        	HashSet<int> result = new HashSet<int>();

 
        	
        	ListA.Sort();
			ListB.Sort();
        	
        	foreach ( int elem in ListA )
        	{
        		if ( ! ListB.Contains ( elem ) )
        			result.Add ( elem );
        	}
        	
        	return result;
        }
        
        public DateTime MinJobStartTime{
        
            get{
                return lastMinStartTime;
            }
        
        
        }

        public Dictionary<int, ISchedulerJob> GetJobs(JobState state)
        {
            IFilterCollection jobFilters = hpcScheduler.CreateFilterCollection();
            jobFilters.Add(FilterOperator.Equal, PropId.Job_State, state);
            ISchedulerCollection jobs = hpcScheduler.GetJobList(jobFilters, null);
            Dictionary<int, ISchedulerJob> jobDict = new Dictionary<int, ISchedulerJob>();
            
            foreach (ISchedulerJob job in jobs)
            {

                updateMinimum(job);

                if (!jobDict.ContainsKey(job.Id))
                {
                    jobDict[job.Id] = job;
                }
            }

            if (state == JobState.Running ) //|| state == JobState.Finished || state == JobState.Finishing )   
                lastMinStartTime = tempMinStartTime;

            return jobDict;
        }

        public Dictionary<int, ISchedulerJob> GetJobsByStartTime()
        {
            return GetJobs(lastMinStartTime);
        }
        public Dictionary<int, ISchedulerJob> GetJobs(DateTime lastDate)
        {
            IFilterCollection jobFilters = hpcScheduler.CreateFilterCollection();
            jobFilters.Add(FilterOperator.GreaterThanOrEqual, PropId.Job_StartTime, lastDate);
            ISchedulerCollection jobs = hpcScheduler.GetJobList(jobFilters, null);
            Dictionary<int, ISchedulerJob> jobDict = new Dictionary<int, ISchedulerJob>();

            foreach (ISchedulerJob job in jobs)
            {
                if (job.State == JobState.Running)
                    updateMinimum(job);
                else
                    continue;

                
                if (!jobDict.ContainsKey(job.Id))
                {
                    jobDict[job.Id] = job;
                }
            }
 
            lastMinStartTime = tempMinStartTime;
            lastGetJobsRun = jobs;

            return jobDict;
        }


 

        private void updateMinimum(ISchedulerJob job)
        {


            if (job.State  == JobState.Running )
                tempMinStartTime = job.StartTime < tempMinStartTime ? job.StartTime : tempMinStartTime;
        }
        
    }
}
