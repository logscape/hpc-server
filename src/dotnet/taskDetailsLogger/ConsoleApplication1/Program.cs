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
    
        static IScheduler scheduler;
        static Dictionary<TaskId, int> tracker  = new Dictionary<TaskId,int>();
        static DateTime iterationStartTime;
        static int lineCounter = 0;
        static DateTime jobMinStartDate;
        static DateTime tempMinStartDate; 
        static DateTime taskMaxEndDate;
        static DateTime tempMaxEndDate; 
        static int loglines = 0;
        static int runningJobPollDuration = 30; 

        static string hpcDateTimeFormat = "dd/MM/yy HH:mm:ss";
        static string outputPath = string.Empty;

        static void Connect( string clusterName )
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


        StringBuilder bufferText = new StringBuilder();
        public static void buffer( string line)
        { 
        
        }

        public static void flush()
        { 
        
        
        }

        static Dictionary<string, string> makeDictionary(DateTime dt, ISchedulerTask task)
        {
            Dictionary<string, string> taskDict = new Dictionary<string, string>();


            List<string> nodes = new List<string>(task.AllocatedNodes);
            string allocatedNodes = string.Join(" ", nodes.ToArray());


            //taskDict["AllocatedCoreIds"] = task.AllocatedCoreIds;
            taskDict["AllocatedNodes"] = allocatedNodes;
            taskDict["ChangeTime"] = task.ChangeTime.ToString(hpcDateTimeFormat);
            taskDict["CommandLine"] = task.CommandLine;
            taskDict["CreateTime"] = task.CreateTime.ToString(hpcDateTimeFormat);
            //taskDict["DependsOn"] = task.DependsOn;
            taskDict["EndTime"] = task.EndTime.ToString(hpcDateTimeFormat);
            //taskDict["EndValue"] = task.EndValue;
            //taskDict["EnvironmentVariables"] = task.EnvironmentVariables;
            taskDict["ErrorMessage"] = task.ErrorMessage;
            taskDict["ExitCode"] = "" + task.ExitCode;
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
            taskDict["StartTime"] = task.StartTime.ToString(hpcDateTimeFormat);
            taskDict["StartValue"] = "" + task.StartValue;
            taskDict["State"] = "" + task.State;
            taskDict["StdErrFilePath"] = task.StdErrFilePath;
            taskDict["StdInFilePath"] = task.StdInFilePath;
            taskDict["StdOutFilePath"] = task.StdOutFilePath;
            taskDict["TaskId"] = "" + task.TaskId;
            taskDict["WorkDirectory"] = task.WorkDirectory;
            

            taskDict["Timestamp"] = dt.ToString(hpcDateTimeFormat);
            taskDict["ElapsedTimeSec"] = "" + (task.EndTime - task.StartTime).Milliseconds;
            return taskDict;

        }


        static Dictionary<string, string> makeDictionary(ISchedulerJob job ,ISchedulerTask task)
        {
            Dictionary<string, string> taskDict = new Dictionary<string, string>();


            List<string> nodes = new List<string>(task.AllocatedNodes);
            string allocatedNodes = string.Join(" ", nodes.ToArray());


            //taskDict["AllocatedCoreIds"] = task.AllocatedCoreIds;
            taskDict["AllocatedNodes"] = allocatedNodes;
            taskDict["ChangeTime"] = task.ChangeTime.ToString(hpcDateTimeFormat);
            taskDict["CommandLine"] = task.CommandLine;
            taskDict["CreateTime"] = task.CreateTime.ToString(hpcDateTimeFormat);
            //taskDict["DependsOn"] = task.DependsOn;
            taskDict["EndTime"] = task.EndTime.ToString(hpcDateTimeFormat);
            //taskDict["EndValue"] = task.EndValue;
            //taskDict["EnvironmentVariables"] = task.EnvironmentVariables;
            taskDict["ErrorMessage"] = task.ErrorMessage;
            taskDict["ExitCode"] = "" + task.ExitCode;
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
            taskDict["StartTime"] = task.StartTime.ToString(hpcDateTimeFormat);
            taskDict["StartValue"] = "" + task.StartValue;
            taskDict["State"] = "" + task.State;
            taskDict["StdErrFilePath"] = task.StdErrFilePath;
            taskDict["StdInFilePath"] = task.StdInFilePath;
            taskDict["StdOutFilePath"] = task.StdOutFilePath;
            taskDict["TaskId"] = "" + task.TaskId;
            taskDict["WorkDirectory"] = task.WorkDirectory;

            List<string> groups = new List<string>(job.NodeGroups.ToArray());
            string nodeGroups = "[" + string.Join(",", groups.ToArray()) + "]";


            taskDict["NodeGroups"] = nodeGroups;

            if (task.EndTime < new DateTime(1970, 1, 1))
                taskDict["ElapsedTimeSec"] = "" + (DateTime.Now - task.StartTime).Milliseconds;
            else
                taskDict["ElapsedTimeSec"] = "" + (task.EndTime - task.StartTime).Milliseconds;

            taskDict["Timestamp"] = DateTime.Now.ToString(hpcDateTimeFormat);
            //taskDict["ElapsedTimeSec"] = "" + (task.EndTime - task.StartTime).Milliseconds;
            return taskDict;

        }

        static Dictionary<string, string> makeDictionary(ISchedulerTask task )
        {
            Dictionary<string, string> taskDict = new Dictionary<string, string>();

     
            List<string> nodes = new List<string>(task.AllocatedNodes);
            string allocatedNodes = string.Join(" ", nodes.ToArray()); 
                

            //taskDict["AllocatedCoreIds"] = task.AllocatedCoreIds;
            taskDict["AllocatedNodes"] = allocatedNodes;
            taskDict["ChangeTime"] = task.ChangeTime.ToString(hpcDateTimeFormat);
            taskDict["CommandLine"] = task.CommandLine;
            taskDict["CreateTime"] = task.CreateTime.ToString(hpcDateTimeFormat);
            //taskDict["DependsOn"] = task.DependsOn;

            if (task.EndTime < new DateTime(1970, 1, 1))
                taskDict["EndTime"] = null;
            else
                taskDict["EndTime"] = task.EndTime.ToString(hpcDateTimeFormat);
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
            taskDict["StartTime"] = task.StartTime.ToString(hpcDateTimeFormat);
            taskDict["StartValue"] = "" + task.StartValue;
            taskDict["State"] = "" + task.State;
            taskDict["StdErrFilePath"] = task.StdErrFilePath;
            taskDict["StdInFilePath"] = task.StdInFilePath;
            taskDict["StdOutFilePath"] = task.StdOutFilePath;
            taskDict["TaskId"] = "" + task.TaskId;
            taskDict["WorkDirectory"] = task.WorkDirectory;



            

            taskDict["Timestamp"] = DateTime.Now.ToString(hpcDateTimeFormat);

            if ( task.EndTime < new DateTime(1970,1,1) )
                taskDict["ElapsedTimeSec"] = "" + ( DateTime.Now - task.StartTime).Milliseconds;
            else 
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


        static string generateTaskLogLine(DateTime dt, ISchedulerTask task)
        {
            /*
                 
                #Timestamp	JobTask	State	TaskName	AllocatedNodes	StartTime	EndTime	ElapsedTimeSec	ExitCode	ErrorMessage	Output		WorkingDirectory    Command
                26/01/2012 14:50:29	8149.1	Failed	Task no. 29	LOGSCAPEHPC04	26/01/2012 14:50:29	26/01/2012 14:50:29	0	1	NULL	NULL	NULL    @I am a useless app                 
             */



            DateTime logDateStamp = new DateTime();
            string jobTaskId = string.Format("{0}.{1}", task.ParentJobId, task.TaskId);
            TimeSpan duration = task.EndTime - task.StartTime;

            List<string> nodes = new List<string>(task.AllocatedNodes);
            string allocatedNodes = string.Join(" ", nodes.ToArray());

            string logline = string.Format("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t{9}\t{10}\t{11}\t{12}", dt.ToString( hpcDateTimeFormat ), task.TaskId, task.State, task.Name, allocatedNodes, task.StartTime, task.EndTime, duration, task.ExitCode, task.ErrorMessage, task.Output, task.WorkDirectory + "_", task.CommandLine);
            logline = logline.Replace("\t\t", "\tNULL");
            logline = logline + "\n" + "Changetime:" + task.ChangeTime;

            List<string> columns = new List<string>() { "Timestamp", "TaskId", "State", "Name", "AllocatedNodes", "StartTime", "EndTime", "ElapsedTimeSec", "ExitCode", "ErrorMessage", "Output", "WorkingDirectory", "CommandLine" };
            logline = mapToString(makeDictionary(task), columns, "\t");

            return logline;
        }



        static string generateTaskLogLine(DateTime dt,  ISchedulerJob job ,ISchedulerTask task)
        {
            /*
                 
                #Timestamp	JobTask	State	TaskName	AllocatedNodes	StartTime	EndTime	ElapsedTimeSec	ExitCode	ErrorMessage	Output		WorkingDirectory    Command
                26/01/2012 14:50:29	8149.1	Failed	Task no. 29	LOGSCAPEHPC04	26/01/2012 14:50:29	26/01/2012 14:50:29	0	1	NULL	NULL	NULL    @I am a useless app                 
             */



            DateTime logDateStamp = new DateTime();
            string jobTaskId = string.Format("{0}.{1}", task.ParentJobId, task.TaskId);
            TimeSpan duration = task.EndTime - task.StartTime;
            List<string> groups = new List<string>(job.NodeGroups.ToArray());
            string nodeGroups = "[" + string.Join(",", groups.ToArray()) + "]";

            List<string> nodes = new List<string>(task.AllocatedNodes);
            string allocatedNodes = string.Join(" ", nodes.ToArray());

            string logline = string.Format("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t{9}\t{10}\t{11}\t{12}\t{13}", dt.ToString(hpcDateTimeFormat), task.TaskId, task.State, task.Name, allocatedNodes, task.StartTime, task.EndTime, duration, task.ExitCode, task.ErrorMessage, task.Output, task.WorkDirectory, nodeGroups, task.Name, task.CommandLine);
            logline = logline.Replace("\t\t", "\tNULL");
            logline = logline + "\n" + "Changetime:" + task.ChangeTime;

            List<string> columns = new List<string>() { "Timestamp", "TaskId", "State", "AllocatedNodes", "StartTime", "EndTime", "ElapsedTimeSec", "ExitCode", "ErrorMessage", "Output", "WorkDirectory", "NodeGroups", "Name" , "CommandLine" };
            logline = mapToString(makeDictionary(job,task), columns, "\t");

            return logline;
        }

        static string generateTaskLogLine(ISchedulerTask task )
        {
                /*
                 
                    #Timestamp	JobTask	State	TaskName	AllocatedNodes	StartTime	EndTime	ElapsedTimeSec	ExitCode	ErrorMessage	Output		WorkingDirectory    Command
                    26/01/2012 14:50:29	8149.1	Failed	Task no. 29	LOGSCAPEHPC04	26/01/2012 14:50:29	26/01/2012 14:50:29	0	1	NULL	NULL	NULL    @I am a useless app                 
                 */
            


               DateTime logDateStamp = new DateTime();
               string jobTaskId =  string.Format ( "{0}.{1}" , task.ParentJobId, task.TaskId);
               TimeSpan  duration =  task.EndTime -  task.StartTime;
               
               List<string>  nodes = new List<string>(  task.AllocatedNodes );
               string allocatedNodes = string.Join(" ", nodes.ToArray() );

               string logline = string.Format("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t{9}\t{10}\t{11}\t{12}", logDateStamp.ToString(hpcDateTimeFormat), task.TaskId, task.State, task.Name, allocatedNodes, task.StartTime, task.EndTime, duration, task.ExitCode, task.ErrorMessage, task.Output, task.WorkDirectory + "_", task.CommandLine);
               logline = logline.Replace("\t\t", "\tNULL");
               logline = logline + "\n" + "Changetime:" + task.ChangeTime;

               List<string> columns = new List<string>() { "Timestamp", "TaskId", "State","Name", "AllocatedNodes", "StartTime", "EndTime", "ElapsedTimeSec", "ExitCode", "ErrorMessage", "Output", "WorkingDirectory", "CommandLine" };
               logline = mapToString(makeDictionary ( task ), columns, "\t");
            
               return logline;  
           }


        static void generateTaskLogLines(DateTime dt,ISchedulerCollection tasks)
        {

            StringBuilder buffer = new StringBuilder();


            foreach (ISchedulerTask task in tasks)
            {
 
                tempMaxEndDate = task.EndTime > tempMaxEndDate ? task.EndTime : tempMaxEndDate;
                ISchedulerCore core;
                ISchedulerNode node;
                
                
                if (tracker.ContainsKey(task.TaskId))
                {
                    continue;
                }

                if (task.EndTime > taskMaxEndDate)
                {
                    tracker[task.TaskId] = 1;
                    buffer.Append(generateTaskLogLine(dt,task));
                    buffer.Append("\n");
                    //System.Console.WriteLine(generateTaskLogLine(task));
                    lineCounter++;
                }
               
            }
            if (buffer.ToString().Length != 0)
                System.Console.WriteLine(buffer.ToString().Trim());
 

        }

        static void generateTaskLogLines(DateTime dt, ISchedulerJob job, ISchedulerCollection tasks)
        {

            StringBuilder buffer = new StringBuilder();


            foreach (ISchedulerTask task in tasks)
            {

                tempMaxEndDate = task.EndTime > tempMaxEndDate ? task.EndTime : tempMaxEndDate;
                ISchedulerCore core;
                ISchedulerNode node;


                if (tracker.ContainsKey(task.TaskId))
                {
                    continue;
                }

                if (task.EndTime > taskMaxEndDate)
                {
                    tracker[task.TaskId] = 1;
                    buffer.Append(generateTaskLogLine(dt,job, task));
                    buffer.Append("\n");
                    //System.Console.WriteLine(generateTaskLogLine(task));
                    lineCounter++;
                }

            }
            if (buffer.ToString().Length != 0)
                System.Console.WriteLine(buffer.ToString().Trim());


        }


           static void generateTaskLogLines(ISchedulerCollection tasks)
           {

               StringBuilder buffer = new StringBuilder();


               foreach (ISchedulerTask task in tasks)
               {

                   tempMaxEndDate = task.EndTime > tempMaxEndDate ? task.EndTime : tempMaxEndDate;

                   if (tracker.ContainsKey(task.TaskId))
                   {
                       continue;
                   }

                   if (task.EndTime > taskMaxEndDate)
                   {
                       tracker[task.TaskId] = 1;
                       buffer.Append ( generateTaskLogLine(task) );
                       //System.Console.WriteLine(generateTaskLogLine(task));
                       lineCounter++;
                  }
                  // System.Console.WriteLine(generateTaskLogLine(task));
               }

               System.Console.WriteLine(buffer.ToString());

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

     
           static ISchedulerCollection getTasksFilterByEndTime(ISchedulerJob job)
           {
               ISchedulerCollection tasks;
               IFilterCollection filters = scheduler.CreateFilterCollection();

               filters.Add(FilterOperator.GreaterThanOrEqual, PropId.Task_EndTime, iterationStartTime);
               tasks = job.GetTaskList(filters, null, true);
               
               return tasks;
           }

           static void createLogDataFrom( DateTime dt)
           {
               IFilterCollection jobFilters = scheduler.CreateFilterCollection();
               jobFilters.Add(FilterOperator.GreaterThanOrEqual, PropId.Job_StartTime, dt);

               ISchedulerCollection jobs = scheduler.GetJobList(jobFilters, null); //getJobs  (jobFilters);

               DateTime now = DateTime.Now;
               foreach (ISchedulerJob job in jobs)
               {


                   generateTaskLogLines(now, job, getTasks(job, TaskState.Failed));
                   generateTaskLogLines(now, job, getTasks(job, TaskState.Finished));

               }
           }


           static void createLogData () 
           {
                   IFilterCollection  jobFilters =   scheduler.CreateFilterCollection();
                   jobFilters.Add ( FilterOperator.GreaterThanOrEqual, PropId.Job_StartTime , jobMinStartDate );

                   ISchedulerCollection jobs = scheduler.GetJobList ( jobFilters , null ); //getJobs  (jobFilters);

                   DateTime now = DateTime.Now;
                   foreach ( ISchedulerJob job in  jobs)
                   {


                       generateTaskLogLines(now,job,getTasks(job, TaskState.Failed));
                       generateTaskLogLines(now,job,getTasks(job, TaskState.Finished));

                   }        
           }

           static Dictionary<string, string> getArgs(string[] args)
           { 
                Dictionary<string,string> arguments = new Dictionary<string,string>();
                string value = string.Empty;
                string currentArg = string.Empty;
                List<string> values = new List<string>();
                bool flag = false;
                foreach ( string arg in args)
                {

                    if (arg[0] == '-')
                    {
                      

                        values = new List<string>();
                
                        string[] elems = arg.Split('-');
                        arguments[elems[1]] = string.Empty;

                        currentArg = elems[1];

                        continue;
                        
                    }
                    else {
                       
                        values.Add(arg);
                        arguments[currentArg] = string.Join(" ", values.ToArray());
                    }


                
                        
                }

                info( string.Format( "Arguments Keys   :{0}",string.Join("\t",arguments.Keys.ToArray() )    ) );
                info( string.Format( "Arguments Values :{0}",string.Join("\t",arguments.Values.ToArray() )  ) );

                return arguments;
           }

           static void getMinimumStartDate()
           {

               taskMaxEndDate = new DateTime(1970, 1, 1);
               tempMaxEndDate = new DateTime(1970, 1, 1);
               jobMinStartDate = new DateTime(3000, 1, 1);
               tempMinStartDate = new DateTime(3000, 1, 1);

               ISchedulerCollection jobs;

               


               while (true)
               {
                   IFilterCollection jobFilter = scheduler.CreateFilterCollection();
                   jobFilter.Add(FilterOperator.Equal, PropId.Job_State, JobState.Running);
                   jobs = scheduler.GetJobList(jobFilter, null);

                   foreach (ISchedulerJob job in jobs)
                   {
                       tempMinStartDate = job.StartTime < tempMinStartDate ? job.StartTime : tempMinStartDate;

                       ISchedulerCollection tasks = getTasks(job, TaskState.Finished);
                       foreach (ISchedulerTask task in tasks)
                       {
                           tempMaxEndDate = task.EndTime > tempMaxEndDate ? task.EndTime : tempMaxEndDate;


                       }
                   }

                   if (jobs.Count > 0)
                   {
                       info( "There are now running jobs. Sleeping (" + runningJobPollDuration + ")" );
                       return;

                   }
                   else
                   {
                       warn ( "There are no running jobs. Sleeping (" + runningJobPollDuration + ")" );
                   }
                   Thread.Sleep(runningJobPollDuration * 1000);

               }

               taskMaxEndDate = tempMaxEndDate;
               jobMinStartDate = tempMinStartDate;
           
           
           }

           static void debug(string line)
           {
               System.Console.Error.WriteLine(DateTime.Now.ToString(hpcDateTimeFormat) + "\t" + "DEBUG" + "\t" + line);
           
           }

           static void warn(string line)
           {

               System.Console.Error.WriteLine(DateTime.Now.ToString(hpcDateTimeFormat) + "\t" + "WARN" + "\t" + line);
           }

           static void info(string line )
           {
               System.Console.Error.WriteLine(DateTime.Now.ToString(hpcDateTimeFormat) + "\t" + "INFO" + "\t" + line);

           }


           static DateTime getMinimumStartDateFromFile(string fileName)
           {
               string dtString = File.ReadAllText(fileName);
               DateTime dt = new DateTime();
               dt = DateTime.Parse(dtString);
               return dt;
           }

           static void resetStartTimes()
           { 
           
           }

           static void Main(string[] args)
           {
               // usage -server ClusterName -username {user}  -password {pwd}  -UseCredentials  -outputPath {path}

               Dictionary<string, string> arguments = getArgs(args);

               int secs = 10;
               string clusterName = "localhost";

               if (arguments.ContainsKey( "scheduler"))
               {
                   clusterName = arguments["scheduler"];
               }

               try
               {
                   if (arguments.ContainsKey("sleep"))
                   {
                       secs = int.Parse(arguments["sleep"]);
                   }

               }
               catch (Exception e)
               {

                    debug(  "\t Invalid sleep argument ! Switching to default (" + secs + "s)"  );
                    debug( e.StackTrace );
               
               }



               try
               {
                   if (arguments.ContainsKey("outputPath"))
                   {
                       outputPath = arguments["outputPath"];
                   }else{
                    
                       outputPath = ".";
                   
                   }
               }
               catch (Exception e)
               {

                   debug("\t Could not use path");
                   debug(e.StackTrace);
               
               }
               bool interuppted = false;
               bool running = true;

               try
               {
                   Connect(clusterName);

               }
               catch (Exception e)
               {
                   debug ( "\t Could not connect to "+ clusterName + " ! Exiting !!!");
                   debug (e.StackTrace);
                   return; 
               }

               string timeStampFile = string.Format("{0}\\taskTimeStampFile.txt",outputPath);


               if (File.Exists(timeStampFile))
               {
                   DateTime dt = getMinimumStartDateFromFile(timeStampFile);
                   if ((DateTime.Now - dt).Hours < 24)
                   {
                       createLogDataFrom(dt);
                   }
                   else {

                       debug("\t  Serice stop time is more than 24hours. ["+ dt +"]");
                   
                   }

               }

  

               getMinimumStartDate();




                if (!File.Exists(timeStampFile))
                {

                    File.Create(timeStampFile).Close();
                        createLogData();
                }


            
               
           //    TextWriter tw = new StreamWriter(new FileStream(timeStampFile, FileMode.Truncate));








               try
               {



                   while (!interuppted && running)
                   {
                       //info( DateTime.Now + "\t" + "[iteration start]");
                       lineCounter = 0;
                       iterationStartTime = DateTime.Now - new TimeSpan(0, 1, 0);

                       createLogData();
                       Thread.Sleep(secs * 1000);
                       //info(DateTime.Now + "\t" + "[iteration end]");
                       taskMaxEndDate = tempMaxEndDate;
                       jobMinStartDate = tempMinStartDate;

               
                       File.WriteAllText(timeStampFile, jobMinStartDate.ToString());
                       


                   }
               }
               catch (Exception e)
               {
                   debug(DateTime.Now + "\t  Fatal Application Error");
                   debug(e.StackTrace);
                   
               }

            
           }
       }

    /*
        TimeStampLogger.update()
        TimeStampLogger.updateWith( dt );
     * 
     
     */

}

