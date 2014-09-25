import java.util.Date
import java.text.SimpleDateFormat

//********


def headNode = headNode == null ? "localhost" : headNode
def timeScope = timeScope == null ? 60 : timeScope


//DEV HIJACKS
//headNode = "logscapeHPC04"
//timeScope = 10

//********


//**** TO BE REMOVED EVENTUALLY
//def fileHandler = new File(".\\work\\"+"MicrosoftHPCApp-1.0"+"\\"+"getTaskDetails.groovy.out")
//***

def prevTaskIds = []
def prevJobIds = []

def sep = "\t"
def sDateFormat = "DD/MM/yyyy HH:mm:ss"

def titleIsNumeric = ['JobTask':false,'State':false,'TaskName':false,'AllocatedNodes':false,'StartTime':false,'EndTime':false,'ElapsedTimeSec':true,'ExitCode':false,'ErrorMessage':false,'Output':false,'CommandLine':false]

while (true) {
	logError ( new Date().toString()  +  sep + "\t[Script Start]")

    def diffTasks = [:]

    def sb = new StringBuilder()
	
	logError ( new Date().toString()  +  sep + "\t[cmdlet(getTaskForRunningJobs) Start]")    
    def sTasksForRunningJob = getOutput("powershell -File getTasksForRunningJobs.ps1",".\\lib")
	logError ( new Date().toString()  +  sep + "\t[cmdlet(getTaskForRunningJobs) End]")
	//def sTasksForRunningJob = getSampleCurrentTasks();

    
    
    def tasksRunningJob = convertToNull(getTasks(sTasksForRunningJob),titleIsNumeric)
    diffTasks.putAll(getNewTasks(tasksRunningJob,prevTaskIds)) // add newly appeared tasks from still running jobs
    
    
    def currJobIds = getJobIds(tasksRunningJob.keySet())
    
    def missingJobIds = getMissingJobIds(currJobIds,prevJobIds)
    
    missingJobIds.each { jid ->
		logError ( new Date().toString()  +  sep + "\t[cmdlet(getTasksForNotRunningAnymoreJobs) Start]")    	
        def sTaskForNotRunningAnymoreJob = getOutput("powershell -File getTasksForNotRunningAnymoreJobs.ps1 "+jid,".\\lib")
		logError ( new Date().toString()  +  sep + "\t[cmdlet(getTasksForNotRunningAnymoreJobs) End]")    		
        //def sTaskForNotRunningAnymoreJob = getSampleNotAnymoreTasks() //TO BE REMOVED
        def tasksNotRunningAnymoreJob = convertToNull(getTasks(sTaskForNotRunningAnymoreJob),titleIsNumeric)
        diffTasks.putAll(getNewTasks(tasksNotRunningAnymoreJob,prevTaskIds)) // add new tasks for job that are no running anymore
    }
    
    def cols = titleIsNumeric.keySet()
    diffTasks.each { task ->
        sb.append(generateLogLine(cols,task,sep,sDateFormat))
    }
    
    //println generateHeader(titleIsNumeric,sep)
    //println generateExpression(titleIsNumeric,sep)
    
    //println sb
    log ( sb )
    //return sb
    
    prevTaskIds = diffTasks.keySet()
    prevJobIds = currJobIds
	
	
	logError ( new Date().toString()  +  sep + "\t[Script End]")	
	logError ( new Date().toString()  +  sep + "\t[Sleep Start]")
    Thread.sleep(1000 * timeScope)
	logError ( new Date().toString()  +  sep + "\t[Sleep End]")	
    
}

return



//*************
def log ( string )
{
   
  //println  string
  System.out << string
}

def logError ( string )
{
	System.err << string << "\n"
}


def getMissingJobIds(currJobIds,oldJobIds) {
    
    def newJobIds = oldJobIds
    newJobIds.removeAll(currJobIds)

    return newJobIds

}

def getJobIds(taskJobs) {

    def jobIds = [:]

    taskJobs.each { taskJob ->
		def jid = getJobId(taskJob)
        jobIds[jid] = ""
    }
    
    return jobIds.keySet()

}

def getJobId(taskJob) {

    if (taskJob.contains("."))
        return taskJob[0..taskJob.indexOf(".")-1].toString()
    else
        return "-1"
}

def getOutput(cmd,workDir) {

    def builder = new ProcessBuilder(cmd.split(' '))
    builder.directory(new File(workDir))
    builder.redirectErrorStream(false)
    def process = builder.start()
    def stdout = process.getInputStream()
    def stderr = process.getErrorStream()
    def stdin = process.getOutputStream().close()
    def stdOutReader = new BufferedReader (new InputStreamReader(stdout))
    def stdErrReader = new BufferedReader (new InputStreamReader(stderr))
    

    def stdOutSb = new StringBuilder()
    def stdErrSb = new StringBuilder()
    
    def line
    
    while ((line = stdOutReader.readLine ()) != null) {
       stdOutSb.append(line).append("\n")
    }
    stdOutSb.append("\n")
    
    
    while ((line = stdErrReader.readLine ()) != null) {
       stdErrSb.append (line).append("\n")
    }
    stdErrSb.append("\n")
    
    process.waitFor()
    
    stdOutReader.close()
    stdErrReader.close()
    
    stdout.close()
    stderr.close()
    
    def sSbOut = stdOutSb.toString()
    def sSbErr = stdErrSb.toString()
    
    if (sSbErr.trim() != "")
        return new Date().toString() + "\n" + sSbErr

    return sSbOut
}


def convertToNull(oldTasksMap,isNum) {

    def newTasksMap = [:]
    
    oldTasksMap.each { jobTask,oldValueMap ->
    
        def newValueMap = [:]
        
        oldValueMap.each { k,v ->
        
            if (v.toString() == "" &&  isNum.containsKey(k))
                newValueMap[k] = isNum[k] == true ? 0 : "NULL"
            else
                newValueMap[k] = v
        
        }
        
        newTasksMap[jobTask] = newValueMap
    
    }

    return newTasksMap

}

def generateHeader(columns,sep) {
    def sb = new StringBuilder()
    def newCols = []
    columns.keySet().each {
        newCols.add(it.replaceAll(" ",""))
    }
    return sb.append("#").append("Timestamp").append(sep).append(newCols.join(sep)).toString()
}


def generateExpression(columns,sep) {
    def sb = new StringBuilder()
    sb.append("(*" + sep + ")").append(sep)
    columns.keySet().each {
        sb.append("(*" + sep + ")").append(sep)
    }
    return sb.toString().trim()
}

def getTasks(out)
{

    def props = [:]
        
    def group = out.split(/(?m)^\n/)
    group.each { g ->
    
        def hashmap = [:]
        def jobTask = ""
        
        g.split("\n").each { l ->
        
            if (l.contains(":")) {
            
                def indexOfCol = l.indexOf(":")
                if (indexOfCol > 0) {
                    def key = l[0..indexOfCol-1].trim().replaceAll(" ","")
                    def value = l.size() > (indexOfCol+1) ? l[indexOfCol+1..-1].trim() : ""
                    
                    if (key == "TaskId")
                        jobTask = value
                    else if (!hashmap.containsKey(key))
                        hashmap[key] = value
                }
            }
        }
        
        if (jobTask != "" && !props.containsKey(jobTask))
            props[jobTask] = hashmap
    }
      
    return props    
}

def generateLogLine(cols,pair,sep,sDateFormat) {

    def jobTask = pair.key
    def oldValueMap = pair.value

    def sb = new StringBuilder()
    
    def now = new Date().format(sDateFormat).toString()
    
    
    //prepare ValueMap to be printed
    def valueMap = [:]
    cols.each { c ->
        if (oldValueMap.containsKey(c)) valueMap[c] = oldValueMap[c]
    }
    if (valueMap.containsKey('ElapsedTimeSec'))
        valueMap['ElapsedTimeSec'] = getElapsedTimeSec(valueMap['StartTime'],valueMap['EndTime'],now,sDateFormat)
    
    
    
    // Append Timestamp
    if (valueMap.containsKey('EndTime') && valueMap['EndTime'] != "NULL") { //if there is a end date choose it for timestamp
        sb.append(valueMap['EndTime'] + sep)
    } else { // otherwise just use current time
        sb.append(now + sep)
    }
    
    //append jobTask
    sb.append(jobTask + sep)
    
    //append valueMap
    sb.append(valueMap.values().join(sep))
    
    //append \n
    sb.append("\n")
    
    return sb.toString()
}

def getElapsedTimeSec(sStart,sEnd,sNow,sDateFormat) {

    def duration = -1

    def dateFormat = new SimpleDateFormat(sDateFormat)
    
    def dateNow = sNow == "NULL" ? dateNow : dateFormat.parse(sNow)
    def dateStart = sStart == "NULL" ? dateNow : dateFormat.parse(sStart)
    def dateEnd = sEnd == "NULL" ? dateNow : dateFormat.parse(sEnd)
        
    use(groovy.time.TimeCategory) {
        duration = (dateEnd - dateStart).seconds.toString()
    }
    
    return duration
}


def getNewTasks(currTasks,prevTaskIds) {
    
    def diffTasks = [:]
 
    currTasks.keySet().each { jobTask ->
    
        if (!prevTaskIds.contains(jobTask)) {
            diffTasks[jobTask] = currTasks[jobTask]
        }
        
    }
    
    return diffTasks

}

def getSampleCurrentTasks() {

        return """
    
    
    
Task Id                         : 6095.56
State                           : Finished
Task Name                       : Task no. 16
Command Line                    : @ping -n  12  127.0.0.1 > nul
Resource Request                : 1-1 cores
Task Type                       : Basic Task
Submit Time                     : 23/01/2012 11:08:10
Start Time                      : 23/01/2012 11:16:25
End Time                        : 23/01/2012 11:16:36
Elapsed Time                    : 00:00:00:11
Total Kernel Time               : 0
Total User Time                 : 15
Working Set                     : 3888 KB
Processes                       : 
Required Nodes                  : 
Allocated Nodes                 : LOGSCAPEHPC04
Pending Reason                  : 
Exit Code                       : 0
Error Message                   : 
Output                          : 


Task Id                         : 6095.52
State                           : Finished
Task Name                       : Task no. 16
Command Line                    : @ping -n  12  127.0.0.1 > nul
Resource Request                : 1-1 cores
Task Type                       : Basic Task
Submit Time                     : 23/01/2012 11:08:10
Start Time                      : 23/01/2012 11:16:25
End Time                        : 23/01/2012 11:16:36
Elapsed Time                    : 00:00:00:11
Total Kernel Time               : 0
Total User Time                 : 15
Working Set                     : 3888 KB
Processes                       : 
Required Nodes                  : 
Allocated Nodes                 : LOGSCAPEHPC04
Pending Reason                  : 
Exit Code                       : 0
Error Message                   : 
Output                          : 


Task Id                         : 6095.53
State                           : Failed
Task Name                       : Task no. 16
Command Line                    : @ping -n  9  127.0.0.1 > nul
Resource Request                : 1-1 cores
Task Type                       : Basic Task
Submit Time                     : 23/01/2012 11:08:10
Start Time                      : 23/01/2012 11:16:36
End Time                        : 
Elapsed Time                    : 00:00:00:08
Total Kernel Time               : 0
Total User Time                 : 0
Working Set                     : 3888 KB
Processes                       :
Required Nodes                  :
Allocated Nodes                 : LOGSCAPEHPC04
Pending Reason                  :
Exit Code                       : 2
Error Message                   : An error message!!
Output                          :




    """
}


def getSamplePreviousTasks() {

    return """
    
Task Id                         : 1111.52
State                           : Finished
Task Name                       : Task no. 16
Command Line                    : @ping -n  12  127.0.0.1 > nul
Resource Request                : 1-1 cores
Task Type                       : Ba!s Task
Submit Time                     : 23/01/2012 11:08:10
Start Time                      : 23/01/2012 11:16:25
End Time                        : 23/01/2012 11:16:36
Elapsed Time                    : 00:00:00:11
Total Kernel Time               : 0
Total User Time                 : 15
Working Set                     : 3888 KB
Processes                       :
Required Nodes                  : 
Allocated Nodes                 : LOGSCAPEHPC04
Pending Reason                  : 
Exit Code                       : 0
Error Message                   : 
Output                          : 


Task Id                         : 6095.52
State                           : Finished
Task Name                       : Task no. 16
Command Line                    : @ping -n  12  127.0.0.1 > nul
Resource Request                : 1-1 cores
Task Type                       : Basic Task
Submit Time                     : 23/01/2012 11:08:10
Start Time                      : 23/01/2012 11:16:25
End Time                        : 23/01/2012 11:16:36
Elapsed Time                    : 00:00:00:11
Total Kernel Time               : 0
Total User Time                 : 15
Working Set                     : 3888 KB
Processes                       :
Required Nodes                  : 
Allocated Nodes                 : LOGSCAPEHPC04
Pending Reason                  : 
Exit Code                       : 0
Error Message                   : 
Output                          : 


    """
}


def getSampleNotAnymoreTasks() {

    return """
    
Task Id                         : 1111.52
State                           : Finished
Task Name                       : Task no. 16
Command Line                    : @ping -n  12  127.0.0.1 > nul
Resource Request                : 1-1 cores
Task Type                       : Ba!s Task
Submit Time                     : 23/01/2012 11:08:10
Start Time                      : 23/01/2012 11:16:25
End Time                        : 23/01/2012 11:16:36
Elapsed Time                    : 00:00:00:11
Total Kernel Time               : 0
Total User Time                 : 15
Working Set                     : 3888 KB
Processes                       :
Required Nodes                  : 
Allocated Nodes                 : LOGSCAPEHPC04
Pending Reason                  : 
Exit Code                       : 0
Error Message                   : 
Output                          : 

    
Task Id                         : 1111.53
State                           : Finished
Task Name                       : Task no. 16
Command Line                    : @ping -n  12  127.0.0.1 > nul
Resource Request                : 1-1 cores
Task Type                       : Ba!s Task
Submit Time                     : 23/01/2012 11:08:10
Start Time                      : 23/01/2012 11:16:25
End Time                        : 23/01/2012 11:16:36
Elapsed Time                    : 00:00:00:11
Total Kernel Time               : 0
Total User Time                 : 15
Working Set                     : 3888 KB
Processes                       :
Required Nodes                  : 
Allocated Nodes                 : LOGSCAPEHPC04
Pending Reason                  : 
Exit Code                       : 0
Error Message                   : 
Output                          : 



    """
}


def propertyMissing(String name) {return null}