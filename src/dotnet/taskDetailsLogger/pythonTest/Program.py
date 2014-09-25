import clr
clr.AddReferenceToFileAndPath("C:\Program Files\Microsoft HPC Pack 2008 R2 SDK\Bin\Microsoft.Hpc.Scheduler.dll")
clr.AddReferenceToFileAndPath("C:\Program Files\Microsoft HPC Pack 2008 R2 SDK\Bin\Microsoft.Hpc.Scheduler.Properties.dll")




def getJobs(state):
	filterCollection =	scheduler.CreateFilterCollection()
	filterCollection.Add( FilterOperator.Equal, Scheduler.PropId.Job_State, JobState.Running )
	return scheduler.GetJobList(filterCollection,None)

def getTasks(job, state ):
	filterCollection =	scheduler.CreateFilterCollection()
	filterCollection.Add( FilterOperator.Equal, Scheduler.PropId.Task_State, state)
	return job.GetTaskList(filterCollection,None)
	




import Microsoft.Hpc.Scheduler as Scheduler 
import Microsoft.Hpc.Scheduler.Properties as Properties
from Microsoft.Hpc.Scheduler.Properties import *
import time

scheduler = Scheduler.Scheduler() 
scheduler.Connect( "logscapeHPC04")


jobs = getJobs(JobState.Running)

print "\n".join ( [ job.Name for job in jobs ] ) 

for job in jobs:
	print job.Name
	tasks = getTasks(job,  TaskState.Finished )
	for task in tasks:
		print "%s \t %s" % (task.Name,task.EndTime)
		
