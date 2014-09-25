import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.*

bundleId = bundleId == null ? "MicrosoftHPCApp-1.0" : bundleId
headNode = headNode == null ? "localhost" : headNode
hpcNodeMetricsWait = hpcNodeMetricsWait == null ? 60 : hpcNodeMetricsWait
hpcNodeMetricsProfiling = hpcNodeMetricsProfiling == null ? false : hpcNodeMetricsProfiling
NaNvalue = NaNvalue == null ? 0 : NaNvalue
pout = pout == null ? System.out : pout
perr = perr == null ? System.err : perr

def stdOut = pout
def stdErr = perr

def sep = "\t"

def toExtract = ['HPCCoresInUse':['*','HPCCoresInUse',''],'HPCDiskThroughput':['*','HPCDiskThroughput','_Total'],'HPCPhysicalMem':['*','HPCPhysicalMem',''],'HPCMemoryPaging':['*','HPCMemoryPaging',''],'HPCNetwork':['*','HPCNetwork','*'],'HPCCpuUsage':['*','HPCCpuUsage','_Total'],'HPCDiskSpace':['*','HPCDiskSpace','_Total']]
def toAgg = ['HPCCoresInUse':['sum'],'HPCDiskThroughput':['sum'],'HPCPhysicalMem':['sum'],'HPCMemoryPaging':['sum'],'HPCNetwork':['sum'],'HPCCpuUsage':['min','avg','max'],'HPCDiskSpace':['min','avg','max']]

//************

stdErr << new Date().toString() + "\t" + "headNode= ["+headNode+"]" + "\n"
stdErr << new Date().toString() + "\t" + "hpcNodeMetricsWait= ["+hpcNodeMetricsWait+"]" + "\n"
stdErr << new Date().toString() + "\t" + "hpcNodeMetricsProfiling= ["+hpcNodeMetricsProfiling+"]" + "\n"
stdErr << new Date().toString() + "\t" + "NaNvalue= ["+NaNvalue+"]" + "\n"

try {

    debug(stdErr,"Start Script")
    
    def command = "powershell -File .\\deployed-bundles\\"+bundleId+"\\lib\\nodeMetrics.ps1 " + headNode + " " + hpcNodeMetricsWait
    debug(stdErr,"Command: " + command)
    
    def builder = new ProcessBuilder(command.split(' '))
    builder.redirectErrorStream(false)
    debug(stdErr,"Executing Command: " + command)
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
    
    if (serviceManager != null) {
        stdErr << new Date().toString() + "\t" + "::::::LifeCycle REGISTERING called::::::" + "\n"
        serviceManager.registerLifeCycleObject(lifeCycle)
    } else {
        stdErr << new Date().toString() + "\t" + "------LifeCycle REGISTERING notcalled------" + "\n"
    }    
    
    def stdout = process.getInputStream()
    def reader = new BufferedReader(new InputStreamReader(stdout))
    
    def ob = new StringBuilder();
    debug(stdErr,"Start Iteration")
    while ((line = reader.readLine ()) != null) {
       //debug(stdErr,"Reading line")
       if (line.trim() == "###") {
            debug(stdErr,"End Iteration")
            def stringOut = ob.toString()
						            
            debug(stdErr,"Start ParsingOutput")
			def properties = parseProperties(col,stringOut)
            debug(stdErr,"End ParsingOutput")
                        
            def interestingPks = getPks(toExtract.values()) // extracts filters to check on what we want to retrieve

            def byNode = [:]

            debug(stdErr,"Start ExtractingMetrics")
            properties.each {
                if (it.containsKey("Value")) { // a valid formatted metric line
                    if (interestingPks.contains(getPk(it.values().toArray()))) { // if we want to retrieve
                        try {
                            def nodeName = it["NodeName"]
                            def metric = it["Metric"]
							def valueStr = it["Value"].toString().trim()
							def value;
							if (valueStr == 'NaN') {
								value = NaNvalue.toFloat()
							}
							else {
								value = (metric == 'HPCDiskSpace') ? (100 - it["Value"].toFloat()) : it["Value"].toFloat()
							}
                            if (! byNode.containsKey(nodeName)) { byNode[nodeName] = [:] }
							if (! byNode[nodeName].containsKey(metric)) {
								byNode[nodeName][metric] = []
							}
							byNode[nodeName][metric].add(value)
                        } catch(e) {
                            stdErr << new Date().toString() + "\t" + e.toString() + "\n"
                        }
                    }
                }
            }
            
			//*** REDUCE (because infos are only for nodes and for instance hpcnetwork return values for each itnerface as there is no _Total counter
            byNode.each { node,metrics ->
                metrics.each { metric,values ->
					byNode[node][metric] = values.sum().toFloat()
                }
            }
            debug(stdErr,"End ExtractingMetrics")

            def agg = [:]
			def nbNodes = 0
            def now = new Date().toString() // so the date will be the same
            
            debug(stdErr,"Start PrintingLogLines")
            byNode.keySet().each { node ->
                def sb = new StringBuilder()
                sb.append(now) // append date at beginning of line
                sb.append(sep + node) // append node
				nbNodes++
                toExtract.keySet().each { metric ->
                     if (byNode[node].containsKey(metric)) {
                            def v = byNode[node][metric].toFloat()
                            sb.append(sep + String.format("%.2f",v)) // print agg value with sep firstREQUIRES
                            //sb.append(sep + metric+":"+byNode[node][metric]) // print agg value with sep firstREQUIRES
                            
                            if (! agg.containsKey(metric)) {
                                agg[metric] = ['min':v,'max':v,'sum':0.0,'c':0,'avg':0.0]
                            }
                            agg[metric]['min'] = v < agg[metric]['min'] ? v : agg[metric]['min']
                            agg[metric]['max'] = v > agg[metric]['max'] ? v : agg[metric]['max']
                            agg[metric]['sum'] += v
                            agg[metric]['c']++
                            agg[metric]['avg'] = agg[metric]['sum'] / agg[metric]['c']
                     }                                       
                }
                sb.append("\n") // append new line character
                stdOut << sb.toString()
            }
            
            def sb = new StringBuilder()
            sb.append(now) // append date at beginning of line
            sb.append(sep + "_Cluster") // append _Cluster
            toExtract.keySet().each {
                sb.append(sep + "") // append empty
            }
            toAgg.each { metric,fcts ->
                fcts.each { fct ->
                    sb.append(sep + String.format("%.2f",agg[metric][fct]))
                }
            }
			sb.append(sep + nbNodes)
            sb.append("\n") // append new line character
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

def debug(er,s) { if (hpcNodeMetricsProfiling == true) er << new Date().format("yyyy/MM/dd HH:mm:ss.SSS").toString() + "\t" + s + "\n" }

//************

def getPk(aPk) {
    def l = []
    
    l.add(aPk[0].trim() != "" ? "*" : "")
    l.add(aPk[1].trim())
    def counter = aPk[2].trim()
    
    if (counter == "_Total") {
        l.add("_Total")
    } else if (counter != "") {
        l.add("*")
    }
    else {
        l.add("")
    }
    
    return l.join("-")
}

def getPks(input) {
    def output = []
    input.each {
        output.add(getPk(it))
    }
    return output
}

def isInteresting(l,ofInterest) {
    return ofInterest.contains(getPk(l))
}

def parseProperties (columns, t )
{
    def result = []
    def text = t.toString()
	    
    def group = text.split(/(?m)^\n/)
		
    group.each { g ->
        def hashmap = [:]
        g.split("\n").each {
            if (it.contains(":")) { 
                def sp = it.split(":")
                def key = sp[0].trim()
                hashmap [ key ] = sp[1].trim()
            }
        }
        result.add(hashmap)
    }
      
    return result    
}

def generateHeader() {
    return "#Timestamp    Node    DiskTpBps    MemAvbMB    MemPagPps    NetworkBps    CpuPct    DiskUsedPct    TotalDiskTpKBps    TotalMemAvbMB    TotalMemPagPps    TotalNetworkBps    MinCpuPct    AvgCpuPct    MaxCpuPct    MinDiskUsedPct    AvgDiskUsedPct    MaxDiskUsedPct"
}

def getSampleMetrics() {
    return """


NodeName   : 
Metric     : HPCClusterCpu
Counter    : AverageAllNodes
Value      : 4.78291
AutoUpdate : False

NodeName   : 
Metric     : HPCClusterDiskThroughput
Counter    : SumAllNodes
Value      : 47239.56
AutoUpdate : False

NodeName   : 
Metric     : HPCClusterNetwork
Counter    : SumAllNodes
Value      : 53895.05
AutoUpdate : False

NodeName   : 
Metric     : HPCSchedulerCores
Counter    : Number of busy cores
Value      : 0
AutoUpdate : False

NodeName   : 
Metric     : HPCSchedulerCores
Counter    : Number of idle cores
Value      : 4
AutoUpdate : False

NodeName   : 
Metric     : HPCSchedulerCores
Counter    : Number of offline cores
Value      : 0
AutoUpdate : False

NodeName   : 
Metric     : HPCSchedulerCores
Counter    : Number of online cores
Value      : 4
AutoUpdate : False

NodeName   : 
Metric     : HPCSchedulerCores
Counter    : Number of unreachable cores
Value      : 0
AutoUpdate : False

NodeName   : 
Metric     : HPCSchedulerCores
Counter    : Total number of cores
Value      : 4
AutoUpdate : False

NodeName   : 
Metric     : HPCSchedulerJobs
Counter    : Number of canceled jobs
Value      : 739
AutoUpdate : False

NodeName   : 
Metric     : HPCSchedulerJobs
Counter    : Number of configuring jobs
Value      : 1
AutoUpdate : False

NodeName   : 
Metric     : HPCSchedulerJobs
Counter    : Number of failed jobs
Value      : 1154
AutoUpdate : False

NodeName   : 
Metric     : HPCSchedulerJobs
Counter    : Number of finished jobs
Value      : 0
AutoUpdate : False

NodeName   : 
Metric     : HPCSchedulerJobs
Counter    : Number of queued jobs
Value      : 0
AutoUpdate : False

NodeName   : 
Metric     : HPCSchedulerJobs
Counter    : Number of running jobs
Value      : 0
AutoUpdate : False

NodeName   : 
Metric     : HPCSchedulerJobs
Counter    : Total number of jobs
Value      : 1894
AutoUpdate : False

NodeName   : 
Metric     : HPCSchedulerNodes
Counter    : Number of draining nodes
Value      : 0
AutoUpdate : False

NodeName   : 
Metric     : HPCSchedulerNodes
Counter    : Number of offline nodes
Value      : 0
AutoUpdate : False

NodeName   : 
Metric     : HPCSchedulerNodes
Counter    : Number of online nodes
Value      : 2
AutoUpdate : False

NodeName   : 
Metric     : HPCSchedulerNodes
Counter    : Number of unreachable nodes
Value      : 0
AutoUpdate : False

NodeName   : 
Metric     : HPCSchedulerNodes
Counter    : Total number of nodes
Value      : 2
AutoUpdate : False

NodeName   : LOGSCAPEHPC04
Metric     : HPCContextSwitches
Counter    : 
Value      : 10394.94
AutoUpdate : False

NodeName   : LOGSCAPEHPC04
Metric     : HPCCoresInUse
Counter    : 
Value      : 0
AutoUpdate : False

NodeName   : LOGSCAPEHPC04
Metric     : HPCCpuUsage
Counter    : _Total
Value      : 6.809758
AutoUpdate : False

NodeName   : LOGSCAPEHPC04
Metric     : HPCDiskQueue
Counter    : _Total
Value      : 0.001084396
AutoUpdate : False

NodeName   : LOGSCAPEHPC04
Metric     : HPCDiskSpace
Counter    : _Total
Value      : 59.9204
AutoUpdate : False

NodeName   : LOGSCAPEHPC04
Metric     : HPCDiskSpace
Counter    : C:
Value      : 59.89182
AutoUpdate : False

NodeName   : LOGSCAPEHPC04
Metric     : HPCDiskSpace
Counter    : HarddiskVolume1
Value      : 71.71717
AutoUpdate : False

NodeName   : LOGSCAPEHPC04
Metric     : HPCDiskThroughput
Counter    : _Total
Value      : 16636.63
AutoUpdate : False

NodeName   : LOGSCAPEHPC04
Metric     : HPCJobsRunning
Counter    : 
Value      : 0
AutoUpdate : False

NodeName   : LOGSCAPEHPC04
Metric     : HPCMemoryPaging
Counter    : 
Value      : 0
AutoUpdate : False

NodeName   : LOGSCAPEHPC04
Metric     : HPCMSMQRequestQueueLength
Counter    : 
Value      : 0
AutoUpdate : False

NodeName   : LOGSCAPEHPC04
Metric     : HPCMSMQResponseQueueLength
Counter    : 
Value      : 0
AutoUpdate : False

NodeName   : LOGSCAPEHPC04
Metric     : HPCMSMQTotalBytes
Counter    : 
Value      : 0
AutoUpdate : False

NodeName   : LOGSCAPEHPC04
Metric     : HPCMSMQTotalMessages
Counter    : 
Value      : 0
AutoUpdate : False

NodeName   : LOGSCAPEHPC04
Metric     : HPCNetwork
Counter    : Intel[R] PRO_1000 MT Network Connection
Value      : 72745.85
AutoUpdate : False

NodeName   : LOGSCAPEHPC04
Metric     : HPCNetwork
Counter    : isatap.ajg.local
Value      : 0
AutoUpdate : False

NodeName   : LOGSCAPEHPC04
Metric     : HPCPhysicalMem
Counter    : 
Value      : 938
AutoUpdate : False

NodeName   : LOGSCAPEHPC04
Metric     : HPCSystemCalls
Counter    : 
Value      : 120935.6
AutoUpdate : False

NodeName   : LOGSCAPEHPC04
Metric     : HPCTasksRunning
Counter    : 
Value      : 0
AutoUpdate : False

NodeName   : LOGSCAPEHPC04
Metric     : HPCWcfBrokerCalls
Counter    : 
Value      : 0
AutoUpdate : False

NodeName   : LOGSCAPEHPC04
Metric     : HPCWcfFailedBrokerCalls
Counter    : 
Value      : 0
AutoUpdate : False

NodeName   : LOGSCAPEHPC04
Metric     : HPCWcfIncomingCalls
Counter    : 
Value      : 0
AutoUpdate : False

NodeName   : LOGSCAPEHPC04
Metric     : HPCWcfRetrievedResults
Counter    : 
Value      : 0
AutoUpdate : False

NodeName   : LOGSCAPEHPC08
Metric     : HPCContextSwitches
Counter    : 
Value      : 166.0011
AutoUpdate : False

NodeName   : LOGSCAPEHPC08
Metric     : HPCCoresInUse
Counter    : 
Value      : 0
AutoUpdate : False

NodeName   : LOGSCAPEHPC08
Metric     : HPCCpuUsage
Counter    : _Total
Value      : 0
AutoUpdate : False

NodeName   : LOGSCAPEHPC08
Metric     : HPCDiskQueue
Counter    : _Total
Value      : 0
AutoUpdate : False

NodeName   : LOGSCAPEHPC08
Metric     : HPCDiskSpace
Counter    : _Total
Value      : 70.17043
AutoUpdate : False

NodeName   : LOGSCAPEHPC08
Metric     : HPCDiskSpace
Counter    : C:
Value      : 70.16668
AutoUpdate : False

NodeName   : LOGSCAPEHPC08
Metric     : HPCDiskSpace
Counter    : HarddiskVolume1
Value      : 71.71717
AutoUpdate : False

NodeName   : LOGSCAPEHPC08
Metric     : HPCDiskThroughput
Counter    : _Total
Value      : 0
AutoUpdate : False

NodeName   : LOGSCAPEHPC08
Metric     : HPCJobsRunning
Counter    : 
Value      : 0
AutoUpdate : False

NodeName   : LOGSCAPEHPC08
Metric     : HPCMemoryPaging
Counter    : 
Value      : 0
AutoUpdate : False

NodeName   : LOGSCAPEHPC08
Metric     : HPCNetwork
Counter    : Intel[R] PRO_1000 MT Network Connection
Value      : 1958.013
AutoUpdate : False

NodeName   : LOGSCAPEHPC08
Metric     : HPCNetwork
Counter    : isatap.ajg.local
Value      : 0
AutoUpdate : False

NodeName   : LOGSCAPEHPC08
Metric     : HPCPhysicalMem
Counter    : 
Value      : 2186
AutoUpdate : False

NodeName   : LOGSCAPEHPC08
Metric     : HPCSystemCalls
Counter    : 
Value      : 522.0034
AutoUpdate : False

NodeName   : LOGSCAPEHPC08
Metric     : HPCTasksRunning
Counter    : 
Value      : 0
AutoUpdate : False



"""


}


/*

Name         : HPCCpuUsage
Description  : The % CPU usage for all processors on the compute node

Name         : HPCPhysicalMem
Description  : Available physical memory on the compute node in MBytes

Name         : HPCMemoryPaging
Description  : The number of page faults per second for a compute node.

Instance     : _Total
Name         : HPCDiskThroughput
DisplayName  : Disk Throughput (Bytes/second)
Description  : The total disk throughput on the compute node

Name         : HPCNetwork
Counter      : Bytes Total/sec
Description  : Network Usage is the rate at which bytes are sent and received over each network adapter, including framing characters. Network Usage is a sum of the bytes Received/sec and bytes Sent/sec.

Name         : HPCDiskSpace
Description  : The free disk space for a disks on the compute node

*/