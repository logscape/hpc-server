import org.codehaus.groovy.tools.shell.commands.ExitCommand;

def headNode = headNode == null ? "localhost" : headNode
long timeScope = timeScope == null ? 60 : timeScope

//HIJACKS****
//headNode = "logscapeHPC04"

timeScope = 2.3

//**************


class JobProgress {

    def static currentRunningJobs = [:]
    def static prevRunningJobs = [:]

}

jobProgress = new JobProgress()
def sep ="\t"



while (true)
{



    def sb = new StringBuilder()
	
    try {
		cmd = "powershell -File .\\deployed-bundles\\MicrosoftHPCApp-1.0\\lib\\jobView.ps1"
		out =  hpcCmd(cmd,".")
		
    }catch ( e ){
		
		logError ( getCurrentTimeStamp() + sep +  "ERROR executing (" + cmd + ")")
		logError ( getCurrentTimeStamp() + sep +  e.printStackTrace() )	
	} 

	
    if (out == null)
    {
      
        if (getLastError != "") return new Date().toString() + "\t" + getLastError
        logError ( getLastError )
        return
    }        



    jobProgress.currentRunningJobs    = parseProperties ( out)     
    finishedJobs          = getFinishedJobs (jobProgress.currentRunningJobs,jobProgress.prevRunningJobs)
    
	 

    out.toString().split("\n").each { line ->

        if ( line.contains ("\t") ){ 

            jobId = line.split("\t")[1]
                sb.append(new Date().toString() + sep)
                .append(line)
                .append("\n")
        }               
    }
    
    finishedJobs.values().each{    elem->

        sb.append(new Date().toString() + sep)
        .append( elem["id"] + sep )
        .append( "##" + sep )
        .append( elem['total'] + sep )        
        .append( "100" + sep )                
        .append( "Finished" + sep )        
        .append( elem["owner"] + sep )        
        .append("\n")
        
    }
    jobProgress.prevRunningJobs = jobProgress.currentRunningJobs
        

    log( sb )
    Thread.sleep ( timeScope * 1000 )
}


def getCurrentTimeStamp()
{
	return new Date().toString()
}

def log ( string )
{
   
  //println  string
  System.out << string 
}

def logError ( string )
{
	System.err << string 	
}


def parseProperties ( text)
{
    def results = [:]
    out.toString().split("\n").each { line ->
        if (line.find("\t"))
        {
            def ret = [:]
            elems = line.split("\t")
            ret["id"] = elems[0]
            ret["running"] = elems[1]
            ret["total"] = elems[2]
            ret["progress"] = elems[3]            
            ret["status"] = elems[4]
            ret["owner"] = elems[5]
            results[ elems[0] ] = ret
        }
        
    }
    
    return results
}

def getFinishedJobs (curr, prev )
{
    result = [:]
 
    prev.keySet().each { key ->
 
        if ( ! curr.keySet().contains( key )  )
        {
            result [ key ] = prev [ key]

        }
        
    
    }
    return result
    
}


def getFinishedJobsAsString (curr, prev )
{
    result = [:]
    def retString  = new StringBuilder()
    prev.keySet().each { key ->
 
        if ( ! curr.keySet().contains( key )  )
        {
            result [ key ] = prev [ key]
            //out =  hpcCmd("","C:\\repository\\trunk\\apps\\MicrosoftHPCApp-1.0\\lib")
            /*
            cmd = "Get-HpcJob  -scheduler logscapeHPC04 -State Finished -id "+ key + ""
            cmdStdOut = hpcCmd("powershell -File jobView.ps1",new File(".\\deployed-bundles\\"+"MicrosoftHPCApp-1.0"+"\\lib").getAbsolutePath())
            retString.append( cmdStdOut)
            retString.append("\n")
            */
            
        }
        
    
    }
    return retString
    
}

def getLastError = ""

def hpcCmd1 (cmd, WorkDir )
{
		cmd  = "powershell -File .\\lib\\jobView.ps1"
		println cmd 
 	    Process process  
 	    try { process = cmd.execute() }  
 	    catch (IOException e) { e.printStackTrace(); return }  
	
 	    Runtime.runtime.addShutdownHook {  
 	        process.destroy()  
 	    }  
 	  
		def  stdOut = new ByteArrayOutputStream()
		def  stdErr = new ByteArrayOutputStream()
		 
 	    process.consumeProcessOutput(stdOut, stdErr)  
        process.waitFor()
		exitCode = process.exitValue()
		
		String outString = new String(stdOut.toByteArray());  
		String errString = new String(stdErr.toByteArray());
		if (errString.length() > 0) System.err.println(new Date().toString() + " Task Error:" + errString)
		if (exitCode != 0) System.err.println(new Date().toString() + " Process Exited with:" + exitCode)
		
		return outString 
 	
	 


}

def hpcCmd(  cmd, workDir ) 
{
    
    //cmd = "-Noninteractive -NoProfile -Command Add-PSSnapIn Microsoft.HPC;" + cmd 

    def builder = new ProcessBuilder(cmd.split(' '))
    //builder.directory(new File(workDir))
    builder.redirectErrorStream(false)

    def process = builder.start()
    def stdout = process.getInputStream()
    def stderr = process.getErrorStream()
    def stdin = process.getOutputStream().close()
    def stdOutReader = new BufferedReader (new InputStreamReader(stdout))
    def stdErrReader = new BufferedReader (new InputStreamReader(stderr))
    

    def stdOutText = new StringBuilder()
    def stdErrText = new StringBuilder()
    
    def line
    while ((line = stdOutReader.readLine ()) != null) {
       stdOutText.append ( line ).append ("\n")
    }
    stdOutText.append("\n")
    
    
    while ((line = stdErrReader.readLine ()) != null) {
       stdErrText.append ( line ).append ("\n")
    }
    stdErrText.append("\n")    
    
    process.waitFor()
    
    stdOutReader.close()    
    stdErrReader.close()        

    
    stdout.close()
    stderr.close()
    
       
    if (stdOutText.toString().trim() == ""){
        //return new Date().toString() + "\tERROR"
        getLastError = "HpcCmd>>ERROR\t" + stdErrText + "\n" + "Cmd>>" + cmd + "\n" + "workdir>>" + workDir + " " + new File(workDir).getAbsolutePath()
		println getLastError 
        return null 
    }

   return stdOutText
}

def propertyMissing(String name) {return null}