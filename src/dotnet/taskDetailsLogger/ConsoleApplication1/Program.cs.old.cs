/*using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

using Microsoft.Hpc.Scheduler;
using Microsoft.Hpc.Scheduler.Properties;

namespace Logscape.Microsoft.Hpc
{


    class Program
    {
        static IScheduler scheduler;
        static Dictionary<TaskId, int> tracker  = new Dictionary<TaskId,int>();
        static DateTime iterationStartTime; 
        static int lineCounter = 0;

        static int loglines = 0;


        static void Connect(  string clusterName )
        {
            scheduler = new Scheduler();



            scheduler.Connect(clusterName);

            
            
           

            Console.Error.WriteLine("Connecting to {0}...", clusterName);
            try
            {
                scheduler.Connect(clusterName);
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Could not connect to the scheduler: {0}", e.Message);
                throw e;
                //abort if no connection could be made
            }

        }





        static void logtaskList ( ISchedulerCollection taskCollection){
            
        
        
        }


        static Dictionary<string, string> makeDictionary(ISchedulerTask task )
        {
            Dictionary<string, string> taskDict = new Dictionary<string, string>();

            string allocatedNodes = string.Empty;// = string.Join(";", task.AllocatedNodes);
            foreach (string s in task.AllocatedNodes)
            {
                allocatedNodes = allocatedNodes + "," + s;
            }

            string dateTimeFormat = "dd/MM/yyyy H:mm:ss";
            taskDict["AllocatedCoreIds"] = task.AllocatedCoreIds;
            taskDict["AllocatedNodes"] = allocatedNodes;
            taskDict["ChangeTime"] = task.ChangeTime.ToString(dateTimeFormat);
            taskDict["CommandLine"] = task.CommandLine;
            taskDict["CreateTime"] = task.CreateTime.ToString(dateTimeFormat);
            //taskDict["DependsOn"] = task.DependsOn;
            taskDict["EndTime"] = task.EndTime.ToString(dateTimeFormat);
            //taskDict["EndValue"] = task.EndValue;
            //taskDict["EnvironmentVariables"] = task.EnvironmentVariables;
            taskDict["ErrorMessage"] = task.ErrorMessage;
            taskDict["ExitCode"] = ""+ task.ExitCode;
            taskDict["MaximumNumberOfCores"] = "" + task.MaximumNumberOfCores;
            taskDict["MaximumNumberOfNodes"] = "" + task.MaximumNumberOfNodes;
            taskDict["MaximumNumberOfSockets"] = "" + task.MaximumNumberOfSockets;
            taskDict["MinimumNumberOfCores"] = "" + task.MinimumNumberOfCores;
            taskDict["MinimumNumberOfNodes"] = "" + task.MinimumNumberOfNodes;
            taskDict["MinimumNumberOfSockets"] = "" + task.MinimumNumberOfSockets;
            taskDict["Name"] = task.Name;
            taskDict["Output"] = task.Output;
            taskDict["ParentJobId"] = "" + task.ParentJobId;
            taskDict["PreviousState"] = "" + task.PreviousState;
            taskDict["RequeueCount"] = "" + task.RequeueCount;
            taskDict["RequiredNodes"] = "" + task.RequiredNodes;
            taskDict["StartTime"] = task.StartTime.ToString(dateTimeFormat);
            taskDict["StartValue"] = "" + task.StartValue;
            taskDict["State"] = "" + task.State;
            taskDict["StdErrFilePath"] = task.StdErrFilePath;
            taskDict["StdInFilePath"] = task.StdInFilePath;
            taskDict["StdOutFilePath"] = task.StdOutFilePath;
            taskDict["TaskId"] = "" + task.TaskId;
            taskDict["WorkDirectory"] = task.WorkDirectory;

            taskDict["Timestamp"] = DateTime.Now.ToString(dateTimeFormat);
            taskDict["ElapsedTimeSec"] = "" +  (task.EndTime -  task.StartTime).Milliseconds;
            return taskDict;

        }

        static string mapToString(Dictionary<string, string> map, List<string> columns , string delimiter)
        { 
            string line = string.Empty;
            foreach ( string c in columns )
            {
                if (map.ContainsKey(c))
                {
                    if (map[c] == null || map[c] == string.Empty)
                    {
                        line = line + "NULL" + delimiter;
                    }
                    else
                    {

                        line = line + map[c] + delimiter;
                    }

                }
                else {

                    line = line + "NULL" + delimiter;
                
                }
            }

            line = line.TrimEnd();
            return line;
        }

        static string generateTaskLogLine(ISchedulerTask task )
        {
                /*
                 
                    #Timestamp	JobTask	State	TaskName	AllocatedNodes	StartTime	EndTime	ElapsedTimeSec	ExitCode	ErrorMessage	Output		WorkingDirectory    Command
                    26/01/2012 14:50:29	8149.1	Failed	Task no. 29	LOGSCAPEHPC04	26/01/2012 14:50:29	26/01/2012 14:50:29	0	1	NULL	NULL	NULL    @I am a useless app                 
                 */
            DateTime logDateStamp = new DateTime();

            /*
            if (task.State == TaskState.Finished)
            {
                logDateStamp = task.EndTime;
            } */


            string jobTaskId =  string.Format ( "{0}.{1}" , task.ParentJobId, task.TaskId);
            TimeSpan  duration =  task.EndTime -  task.StartTime;
            string allocatedNodes = string.Empty;// = string.Join(";", task.AllocatedNodes);
            foreach (string s in task.AllocatedNodes)
            { 
                    allocatedNodes = allocatedNodes + ","  + s; 
            }
            string logline = string.Format("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t{9}\t{10}\t{11},\t{12}", logDateStamp.ToString("dd/MM/yy H:mm:ss"), task.TaskId, task.State, task.Name, allocatedNodes, task.StartTime, task.EndTime, duration, task.ExitCode, task.ErrorMessage, task.Output, task.WorkDirectory + "_", task.CommandLine);
            logline = logline.Replace("\t\t", "\tNULL");
            logline = logline + "\n" + "Changetime:" + task.ChangeTime;

            List<string> columns = new List<string>() { "Timestamp", "TaskId", "State","Name", "AllocatedNodes", "StartTime", "EndTime", "ElapsedTimeSec", "ExitCode", "ErrorMessage", "Output", "WorkingDirectory", "CommandLine" };
            logline = mapToString(makeDictionary ( task ), columns, "\t");
            
            return logline;  
        }


        static void generateTaskLogLines(ISchedulerCollection tasks)
        {


            foreach (ISchedulerTask task in tasks)
            {
                if (tracker.ContainsKey(task.TaskId))
                {
                    continue;
                }
                
              if (task.EndTime > iterationStartTime)
                {
                    tracker[task.TaskId] = 1;
                    System.Console.WriteLine(generateTaskLogLine(task));
                    lineCounter++;
               }
               // System.Console.WriteLine(generateTaskLogLine(task));
            }

        }


        static void generateTaskLogLines(Dictionary<string,object>[] tasks)
        {


            foreach (Dictionary<string,object> task in tasks)
            {
                if (tracker.ContainsKey((TaskId)task["TaskId"]))
                {
                    continue;
                }

                if ( (DateTime)task["EndTime"] > iterationStartTime)
                {
                    tracker[ (TaskId)task["TaskId"]] = 1;
                 //   System.Console.WriteLine(generateTaskLogLine(task));
                    System.Console.WriteLine("TaskId {0}", task["TaskId"]);
                    lineCounter++;
                }
                // System.Console.WriteLine(generateTaskLogLine(task));
            }

        }

        static void generateTaskLogLines(ISchedulerTask[] tasks)
        {


            foreach (ISchedulerTask task in tasks)
            {
                if (tracker.ContainsKey(task.TaskId))
                {
                    continue;
                }

                if (task.EndTime > iterationStartTime)
                {
                    tracker[task.TaskId] = 1;
                    System.Console.WriteLine(generateTaskLogLine(task));
                    lineCounter++;
                }
                // System.Console.WriteLine(generateTaskLogLine(task));
            }
        }



        static ISchedulerCollection getTasks(ISchedulerJob job, TaskState state)
        {
            ISchedulerCollection tasks;
            IFilterCollection filters = scheduler.CreateFilterCollection();

            filters.Add(FilterOperator.Equal, PropId.Task_State, state);
            //  filters.Add(FilterOperator.GreaterThanOrEqual, PropId.Task_EndTime, iterationStartTime);


            tasks = job.GetTaskList(filters, null, true);
           

            return tasks;
        }

        static Dictionary<string, object> bakeDictionary(ISchedulerTask task)
        {
            Dictionary<string, object> taskDict = new Dictionary<string, object>();

        
            
            taskDict["EndTime"] = task.EndTime;
            taskDict["Name"] = task.Name;
            taskDict["TaskId"] = task.TaskId;
            taskDict["State"] = task.State;
            taskDict["AllocatedNodes"] = task.AllocatedNodes;
            taskDict["StartTime"]  = task.StartTime;
            taskDict["EndTime"] = task.EndTime;
            taskDict["ExitCode"] =  task.ExitCode;
            taskDict["ErrorMessage"] =  task.ErrorMessage;
            taskDict["Output"] = task.Output; 
            taskDict["WorkingDirectory"] =  task.WorkDirectory;
            taskDict["CommandLine"] = task.CommandLine;



            return taskDict;
        
        }

        static ISchedulerCollection getTasksFilterByEndTime(ISchedulerJob job)
        {
            ISchedulerCollection tasks;
            IFilterCollection filters = scheduler.CreateFilterCollection();

            filters.Add(FilterOperator.GreaterThanOrEqual, PropId.Task_EndTime, iterationStartTime);


            tasks = job.GetTaskList(filters, null, true);




            return tasks;
        }


        static ISchedulerJob[] getTaskArrayFilterByEndTime(ISchedulerJob job)
        {
            ISchedulerJob[] jobs = null;
            ISchedulerCollection tasks;
            IFilterCollection filters = scheduler.CreateFilterCollection();

            filters.Add(FilterOperator.GreaterThanOrEqual, PropId.Task_EndTime, iterationStartTime);
            tasks = job.GetTaskList(filters, null, true);

            //tasks.CopyTo(jobs);


            return jobs;

            
             
        }

        static Dictionary<string,object>[] getTaskArrayFilterByState(ISchedulerJob job, TaskState state)
        {
            ISchedulerTask[] taskArray;
            ISchedulerCollection tasks;
            IFilterCollection filters = scheduler.CreateFilterCollection();
            

            filters.Add(FilterOperator.GreaterThanOrEqual, PropId.Task_State , state);
            tasks = job.GetTaskList(filters, null, true);

            //tasks.CopyTo(jobs);
            taskArray = new ISchedulerTask[tasks.Count];


            tasks.CopyTo(taskArray, 0);


            Dictionary<string,object>[] taskDictArray = new Dictionary<string,object>[tasks.Count];
            List< Dictionary<string,object> >  taskList = new List< Dictionary<string,object> >();
            foreach (ISchedulerTask task in taskArray)
            {
                taskList.Add(bakeDictionary(task));
            }

            System.Console.WriteLine("\t\t ** Job {0} has {1}, State = {2}",job.Id, tasks.Count, state);
            taskList.CopyTo(taskDictArray);
            int i;
            return taskDictArray;



        }



        


        static void createLogData () 
        {
                IFilterCollection  jobFilters =   scheduler.CreateFilterCollection();

                jobFilters.Add ( FilterOperator.Equal, PropId.Job_State ,  JobState.Running );

                ISchedulerCollection jobs =   scheduler.GetJobList ( jobFilters , null ); //getJobs  (jobFilters);
            

                foreach ( ISchedulerJob job in  jobs)
                {
                    ISchedulerCollection tasks;// = getTasks(job, TaskState.Finished);

                   // generateTaskLogLines(getTasks(job, TaskState.Failed));
                    //generateTaskLogLines(getTasks(job, TaskState.Finished));

                    //generateTaskLogLines(getTasksFilterByEndTime(job));

                    Dictionary<string, object>[] taskDict = getTaskArrayFilterByState(job, TaskState.Finished);

                    generateTaskLogLines(taskDict);

                    System.Console.Error.WriteLine( "\t ** " + DateTime.Now  + "\t" +  "jobId" +   job.Id  +"\t" + job.GetCounters().FinishedTaskCount + "\t" + job.GetCounters().FailedTaskCount );

                }        
        }



        static void Main(string[] args)
        {
            // usage -server ClusterName -username {user}  -password {pwd}  -UseCredentials
            string clusterName = "logscapeHPC04";
            bool interuppted = false;
            bool running = true;

            Connect(clusterName);


            while (!interuppted && running)
            {
                lineCounter = 0;
                iterationStartTime = DateTime.Now;
                Thread.Sleep(10 * 1000);
                createLogData ();
                Thread.Sleep(10 * 1000);
   
                System.Console.Error.WriteLine ( DateTime.Now + "\t" + "loglineCounter:" + lineCounter + " Starttime:" + iterationStartTime + ", Duration" +  (DateTime.Now - iterationStartTime).Seconds);
               
                

            }
            
            
        }
    }

}
*/