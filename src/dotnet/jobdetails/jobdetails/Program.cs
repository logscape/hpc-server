using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;

using Microsoft.Hpc.Scheduler;
using Microsoft.Hpc.Scheduler.Properties;


namespace Logscape.Microsoft.Hpc
{
    class Program
    {
        static int runningJobPollDuration = 30;
        static string hpcDateTimeFormat = "dd/MM/yy HH:mm:ss";
        static string outputPath = string.Empty;

        public static string getLogLine( ISchedulerJob job )
        {
/* #date	time	JobId   Queed   Running	Finished Failed Total	Progress	User
Tue Jan 24 15:13:05 GMT 2012	7016    1	12	30	40	Running	AJG\gomoz

Tue Jan 24 15:13:05 GMT 2012	7017    1	8	23	34.7826086956522	Running	AJG\gomoz

Tue Jan 24 15:13:05 GMT 2012	7018    11	8	28	28.5714285714286	Running	AJG\gomoz

Tue Jan 24 15:13:05 GMT 2012	7019    111	4	15	26.6666666666667	Running	AJG\gomoz

Tue Jan 24 15:13:29 GMT 2012	7016    12  14	30	46.6666666666667	Running	AJG\gomoz */
   //         System.Console.WriteLine("{0}\t{1}\t{2}\t{3}\T{4}", dateStamp, job.Id, job.);

            DateTime dateStamp = DateTime.Now;
            if (job.State == JobState.Finished)
            {
                dateStamp = job.EndTime;
            }

            if (job.State == JobState.Canceled)
            {
                dateStamp = job.EndTime;
            }


            ISchedulerJobCounters counters = job.GetCounters();
            List<string> groups = new List<string>(job.NodeGroups);
            float progress = 100 * ((float)counters.FinishedTaskCount / (float)counters.TaskCount);
            string nodeGroups = "["+string.Join(",", groups.ToArray())+"]";

            string line = string.Empty;

            if (job.State == JobState.Running)
            {
                if (job.EndTime < new DateTime(1970, 1, 1))
                    line = string.Format("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t{9}\t{10}\t{11}\t{12}\t{13}\t{14}", dateStamp.ToString(hpcDateTimeFormat), job.Id,   counters.QueuedTaskCount, counters.RunningTaskCount, counters.FinishedTaskCount, counters.FailedTaskCount, counters.TaskCount, progress, job.State, job.Owner, nodeGroups, job.StartTime, "NULL", (DateTime.Now - job.StartTime).Minutes, job.Name);
                else
                    line = string.Format("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t{9}\t{10}\t{11}\t{12}\t{13}\t{14}", dateStamp.ToString(hpcDateTimeFormat), job.Id, counters.QueuedTaskCount, counters.RunningTaskCount, counters.FinishedTaskCount, counters.FailedTaskCount, counters.TaskCount, progress, job.State, job.Owner, nodeGroups, job.StartTime, job.EndTime, (DateTime.Now - job.StartTime).Minutes, job.Name);
            }
            else if (job.State == JobState.Finished)
            {
                line = string.Format("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t{9}\t{10}\t{11}\t{12}\t{13}\t{14}", dateStamp.ToString(hpcDateTimeFormat), job.Id, counters.QueuedTaskCount, counters.RunningTaskCount, counters.FinishedTaskCount, counters.FailedTaskCount, counters.TaskCount, progress, job.State, job.Owner, nodeGroups, job.StartTime, job.EndTime, (job.EndTime - job.StartTime).Minutes, job.Name);
            }
            else if (job.State == JobState.Failed)
            {
                line = string.Format("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t{9}\t{10}\t{11}\t{12}\t{13}\t{14}", dateStamp.ToString(hpcDateTimeFormat), job.Id, counters.QueuedTaskCount, counters.RunningTaskCount, counters.FinishedTaskCount, counters.FailedTaskCount, counters.TaskCount, progress, job.State, job.Owner, nodeGroups, job.StartTime, job.EndTime, (job.EndTime - job.StartTime).Minutes, job.Name);
            }
            else {
 
                if (job.EndTime < new DateTime(1970, 1, 1))
                    line = string.Format("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t{9}\t{10}\t{11}\t{12}\t{13}\t{14}", dateStamp.ToString(hpcDateTimeFormat), job.Id,  counters.QueuedTaskCount, counters.RunningTaskCount, counters.FinishedTaskCount, counters.FailedTaskCount, counters.TaskCount, progress, job.State, job.Owner, nodeGroups, job.StartTime, "NULL", 0, job.Name);            
                else
                    line = string.Format("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t{9}\t{10}\t{11}\t{12}\t{13}\t{14}", dateStamp.ToString(hpcDateTimeFormat), job.Id,   counters.QueuedTaskCount, counters.RunningTaskCount, counters.FinishedTaskCount, counters.FailedTaskCount, counters.TaskCount, progress, job.State, job.Owner, nodeGroups, job.StartTime, job.EndTime, 0, job.Name);            
            }



            //System.Console.WriteLine( line.Trim() );

            return line.Trim();
        }

        static Dictionary<string, string> getArgs(string[] args)
        {
            Dictionary<string, string> arguments = new Dictionary<string, string>();
            string value = string.Empty;
            string currentArg = string.Empty;
            List<string> values = new List<string>();
            bool flag = false;
            foreach (string arg in args)
            {

                if (arg[0] == '-')
                {


                    values = new List<string>();

                    string[] elems = arg.Split('-');
                    arguments[elems[1]] = string.Empty;

                    currentArg = elems[1];

                    continue;

                }
                else
                {

                    values.Add(arg);
                    arguments[currentArg] = string.Join(" ", values.ToArray());
                }

			


            }

            info(string.Format("Arguments Keys   :{0}", string.Join("\t", arguments.Keys.ToArray())));
            info(string.Format("Arguments Values :{0}", string.Join("\t", arguments.Values.ToArray())));

            return arguments;
        }

        static void debug(string line)
        {
            System.Console.Error.WriteLine(DateTime.Now.ToString(hpcDateTimeFormat) + "\t" + "DEBUG" + "\t" + line);

        }

        static void warn(string line)
        {

            System.Console.Error.WriteLine(DateTime.Now.ToString(hpcDateTimeFormat) + "\t" + "WARN" + "\t" + line);
        }

        static void info(string line)
        {
            System.Console.Error.WriteLine(DateTime.Now.ToString(hpcDateTimeFormat) + "\t" + "INFO" + "\t" + line);

        }


        static void getLastRunningJobs()
        { 
                
        
        }

        static DateTime getMinimumStartDateFromFile(string fileName)
        {
            string dtString = File.ReadAllText(fileName);
            DateTime dt = new DateTime();
            dt = DateTime.Parse(dtString);
            return dt;
        }

        static int Main(string[] args)
        {
            Dictionary<string, string> arguments = getArgs(args);

            string userName = string.Empty;
            string password = string.Empty;



            int secs = 60;
            string clusterName = "localhost";

            if (arguments.ContainsKey("scheduler"))
            {
                clusterName = arguments["scheduler"];
            }

            try
            {
                if (arguments.ContainsKey("wait"))
                {
                    secs = int.Parse(arguments["wait"]);
                }

            }
            catch (Exception e)
            {

                warn("" + DateTime.Now.ToString() + "\t Invalid sleep argument ! Switching ti default (" + secs + ")");
                warn(e.StackTrace);

            }

            Dictionary<int, ISchedulerJob> prevRunningJobs = new Dictionary<int,ISchedulerJob>();
            Dictionary<int, ISchedulerJob> runningJobs  = new Dictionary<int,ISchedulerJob>();

            HashSet<int> prev = new HashSet<int>();
            HashSet<int> curr = new HashSet<int>();
            HashSet<int> diff = new HashSet<int>();


            IScheduler scheduler = new Scheduler();



            try
            {
                scheduler.Connect(clusterName);
            }
            catch (Exception e)
            {
                System.Console.Error.WriteLine("" + DateTime.Now.ToString() + "\t Could not connect to " + clusterName + " ! Exiting !!!");
                System.Console.Error.WriteLine(e.StackTrace);
                return -1;
            }



            try
            {
                if (arguments.ContainsKey("outputPath"))
                {
                    outputPath = arguments["outputPath"];

                    if (!File.Exists(outputPath))
                    {
                        outputPath = ".";
                    }

                }
                else
                {

                    outputPath = "."; outputPath = ".";

                }



            }
            catch (Exception e)
            {

                debug("\t Could not use path");
                debug(e.StackTrace);

            }


            string timeStampFile = string.Format("{0}\\jobTimeStampFile.txt", outputPath);
            debug ("\t  timestampfile: path="+ timeStampFile + "");


            if (File.Exists(timeStampFile))
            {
                DateTime dt = getMinimumStartDateFromFile(timeStampFile);
                debug("\t Using date from  timestampfile: path=" + timeStampFile + "\n" + "\t  Time Used : " + dt.ToString() );
                if  ( ( DateTime.Now - dt ).Hours < 24)
                {
                    Grid tempGrid = new Grid(scheduler);
                    Dictionary<int, ISchedulerJob> jobs = tempGrid.GetJobs(dt);
                    ISchedulerCollection jobValues = tempGrid.lastGetJobsRun;
                   // foreach (ISchedulerJob job in jobs.Values)
                     foreach (ISchedulerJob job in jobValues)
                    {
                        System.Console.WriteLine(getLogLine(job).Trim());
                    }


                }
                else
                {

                    debug("\t  Serice stop time is more than 24hours. [" + dt + "]");

                }
            }


          


            Grid grid = new Grid (scheduler);

            try { 

                bool pollingGridFlag = true;
                while (pollingGridFlag)
                {
                    runningJobs = grid.GetJobs(JobState.Running);


                    if (runningJobs.Count > 0)
                    {
                        pollingGridFlag = false;
                        info("There are now running jobs. Sleeping (" + runningJobPollDuration + ")");
                        continue;
                    }
                    else
                    {
                        warn("There are no running jobs. Sleeping (" + runningJobPollDuration + ")");
                    }

                    Thread.Sleep(runningJobPollDuration * 1000);

                }

                while( true) 
                {
                    Dictionary<int, ISchedulerJob> allJobs = new Dictionary<int, ISchedulerJob>();
                    List<int> jobIds = new List<int>();
                    prevRunningJobs = runningJobs;
                    runningJobs = grid.GetJobsByStartTime();


                    
                    
                    prev = curr;
                    curr = new HashSet<int>(runningJobs.Keys);
                    // diff = new HashSet<int>(prev.Except(curr));
                    diff = grid.getSetDifference( prev,curr);


                    
                    foreach (int jobId in diff)
                    { 
                        // print log line
                        ISchedulerJob job = prevRunningJobs [jobId];
                        job.Refresh();
                        //System.Console.WriteLine(getLogLine(job).Trim());
                        allJobs[job.Id] = job;
                        jobIds.Add(job.Id);
                    }

                    
                    
                    
                    foreach ( ISchedulerJob job  in runningJobs.Values )
                    {

                        //System.Console.WriteLine(getLogLine(job).Trim());

                        allJobs[job.Id] = job;
                        jobIds.Add(job.Id);


                    }

                     List<ISchedulerJob> jobs =  new List<ISchedulerJob> ( grid.GetJobs( JobState.Queued ).Values.ToArray() );

                    foreach (ISchedulerJob job in  jobs ) 
                    {
                        //System.Console.WriteLine(getLogLine(job).Trim().Trim());                
                        allJobs[job.Id] = job;
                        jobIds.Add(job.Id);

                    }

                    jobIds.Sort(new JobComparer(allJobs));

                    foreach (int jobId in jobIds)
                    {
                        ISchedulerJob job = allJobs[jobId];
                        System.Console.WriteLine(getLogLine(job).Trim());
                    }

                    File.WriteAllText(timeStampFile, grid.MinJobStartTime.ToString());
                    Thread.Sleep(secs * 1000);
                

                }

            }catch ( Exception e )
            {
                debug ( "\t + " );  
            
            }





            return 0;
        }
    }
    class JobComparer : IComparer<int>
    {
        Dictionary<int, ISchedulerJob> allJobs;

        public JobComparer(Dictionary<int, ISchedulerJob> allJobs)
        {
            this.allJobs = allJobs;
        }

        public int Compare(int jobIdA, int jobIdB) 
        {
            ISchedulerJob jobA = allJobs[jobIdA];
            ISchedulerJob jobB = allJobs[jobIdB];
            return jobA.EndTime.CompareTo(jobB.EndTime);
        }
    }
}
