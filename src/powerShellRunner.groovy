import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.*

isStandalone = bundleId == null

cwd = isStandalone  ? "." : ".\\deployed-bundles\\"

bundleId = isStandalone  ? "MicrosoftHPCApp-1.1" : bundleId

libPath = isStandalone ? "lib" : ".\\deployed-bundles\\" + bundleId + "\\lib"

def shellscript = args.length > 0 ? "\\" + args[0] : "\\clusterOverview.ps1 "

headNode = isStandalone ? "localhost" : headNode

NaNvalue = NaNvalue == null ? 0 : NaNvalue

pout = isStandalone  ? System.out : pout
perr = isStandalone  ? System.err : perr

def stdOut = pout
def stdErr = perr

def sep = "\t"
isDebugging = true



try {

    debug(stdErr,"Start Script")

    def command = "powershell -File " +  libPath + shellscript 


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
		stdOut << line << "\n"
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

def debug(er,s) { 
	if (true){
		er << new Date().format("yyyy/MM/dd HH:mm:ss.SSS").toString() + "\t" + s + "\n"
	}

}

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

