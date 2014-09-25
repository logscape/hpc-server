import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.*

bundleId = bundleId == null ? "MicrosoftHPCApp-1.0" : bundleId
headNode = headNode == null ? "localhost" : headNode
hpcClusterOverviewWait = hpcClusterOverviewWait == null ? 60 : hpcClusterOverviewWait
hpcClusterOverviewProfiling = hpcClusterOverviewProfiling == null ? false : hpcClusterOverviewProfiling
pout = pout == null ? System.out : pout
perr = perr == null ? System.err : perr

def stdOut = pout
def stdErr = perr

def sep = "\t"

def col = ["RunningJobCount","QueuedJobCount","RunningTaskCount","QueuedTaskCount","BusyCoreCount","IdleCoreCount","TotalCoreCount","FinishedJobCount","FailedJobCount","FinishedTaskCount","FailedTaskCount","ReadyNodeCount","DrainingNodeCount","UnreachableNodeCount","TotalNodeCount"]

//*******

stdErr << new Date().toString() + "\t" + "headNode= ["+headNode+"]" + "\n"
stdErr << new Date().toString() + "\t" + "hpcClusterOverviewWait= ["+hpcClusterOverviewWait+"]" + "\n"
stdErr << new Date().toString() + "\t" + "hpcClusterOverviewProfiling= ["+hpcClusterOverviewProfiling+"]" + "\n"

try {

    debug(stdErr,"Start Script")
    
    def command = "powershell -File .\\deployed-bundles\\"+bundleId+"\\lib\\clusterOverview.ps1 " + headNode + " " + hpcClusterOverviewWait
	debug(stdErr,"Command: " + command)
	
	def builder = new ProcessBuilder(command.split(' '))
    builder.redirectErrorStream(false)
    def process = builder.start()
    process.getOutputStream().close()
    process.getErrorStream().close()
	
    LifeCycle lifeCycle = new LifeCycle() {
        public void start() {
			stdErr << new Date().toString() + "\t" + "::::::LifeCycle START called::::::" + "\n"
		}
        public void stop() {
            process.destroy()
            stdErr << new Date().toString() + "\t" + "::::::LifeCycle STOP called::::::" + "\n"
        }
        
    };
    
    if (serviceManager != null)
    {
        stdErr << new Date().toString() + "\t" + "::::::LifeCycle REGISTERING called::::::" + "\n"
        serviceManager.registerLifeCycleObject(lifeCycle)
    } else {
        stdErr << new Date().toString() + "\t" + "------LifeCycle REGISTERING notcalled------" + "\n"
    }
    
    
    def procOut = process.getInputStream()
    def reader = new BufferedReader(new InputStreamReader(procOut))
    
    def ob = new StringBuilder();
    debug(stdErr,"Start Iteration")
    while ((line = reader.readLine ()) != null) {
       if (line.trim() == "###") {
            debug(stdErr,"End Iteration")
            def stringOut = ob.toString()
           
            debug(stdErr,"Start ParsingOutput")
            def properties = parseProperties(col,stringOut)
            debug(stdErr,"End ParsingOutput")
            
            debug(stdErr,"Start PrintingLogLine")
            def sb = new StringBuilder()
            sb.append(new Date().toString())
            sb.append(toRowString(col, properties, sep ))
            sb.append("\n")
            stdOut << sb.toString()
            debug(stdErr,"End PrintingLogLine")
           
            ob = new StringBuilder()
            debug(stdErr,"Start Iteration")
       } else {
            ob.append(line + "\n")
       }
    }
    
    process.waitFor()
} catch(e) {
    stdErr << new Date().toString() + "\t" + e.toString() + "\n\t" + e.getStackTrace().join("\n\t") + "\n"
	throw e
} finally {
	debug(stdErr,"End Script")
	stdErr << new Date().toString() + "\t" + "Entered finally" + "\n"
}

return

//************

def propertyMissing(String name) {return null}

def debug(er,s) { if (hpcClusterOverviewProfiling == true) er << new Date().format("yyyy/MM/dd HH:mm:ss.SSS").toString() + "\t" + s + "\n" }

//************

def parseProperties (columns, t )
{
    def result = [:]
    def text = t.toString()
    
    text.split("\n").each() { row ->

        elems = row.split(":").collect(){ it-> it.trim() }
        

        if ( columns.contains ( elems[0] )  ) 
        {
            result [ elems [0]  ]  = elems [1]           
        }
            

    }
    
    return result
    
    
}


def toRowString( col, map , sep )
{
    ret = new StringBuilder()
    
    col.each(){ key -> 
        ret.append (sep + map[key]) 
    }
    
    return ret.toString()
}


def getSampleOverview() {

    return """



ClusterName          : logscapeHPC04
Version              : 3.0.2369.0
TotalNodeCount       : 3
ReadyNodeCount       : 3
OfflineNodeCount     : 0
DrainingNodeCount    : 0
UnreachableNodeCount : 0
TotalCoreCount       : 6
BusyCoreCount        : 6
IdleCoreCount        : 0
OfflineCoreCount     : 0
TotalJobCount        : 4719
ConfigJobCount       : 1
SubmittedJobCount    : 0
ValidatingJobCount   : 0
QueuedJobCount       : 127
RunningJobCount      : 6
FinishingJobCount    : 0
FinishedJobCount     : 1109
CancelingJobCount    : 0
CanceledJobCount     : 103
FailedJobCount       : 3373
TotalTaskCount       : 3345762
ConfiguringTaskCount : 3141
SubmittedTaskCount   : 0
QueuedTaskCount      : 602923
RunningTaskCount     : 0
FinishedTaskCount    : 2717849
CancelingTaskCount   : 0
CanceledTaskCount    : 0
FailedTaskCount      : 21844


"""

}