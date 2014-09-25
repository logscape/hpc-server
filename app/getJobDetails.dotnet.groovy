import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.*

bundleId = bundleId == null ? "MicrosoftHPCApp-1.0" : bundleId
headNode = headNode = null ? "localhost" : headNode
jobDetailsWait = jobDetailsWait = null ? 60 : jobDetailsWait
pout = pout == null ? System.out : pout
perr = perr == null ? System.err : perr

def stdOut = pout
def stdErr = perr

def sep = "\t"

//*****

stdErr << new Date().toString() + "\t" + "headNode= ["+headNode+"]" + "\n"
stdErr << new Date().toString() + "\t" + "jobDetailsWait= ["+jobDetailsWait+"]" + "\n"

try {
	def dt = new Date()
	perr << "" + dt.toString() + sep + ""

	def process = (".\\deployed-bundles\\"+bundleId+"\\getJobDetails.exe -scheduler " +headNode+ " -wait "+jobDetailsWait+" -outputPath .\\deployed-bundles\\"+bundleId).execute()

	LifeCycle lifeCycle = new LifeCycle(){
		
		
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
	
	process.consumeProcessOutput(pout,perr)
	process.waitFor()
	
}catch(Exception e)
{
    stdErr << new Date().toString() + "\t" + e.toString() + "\n\t" + e.getStackTrace().join("\n\t") + "\n"
	throw e
} finally {
    stdErr << new Date().toString() + "\t" + "Entered finally" + "\n"
}
	
	
return

//******

def propertyMissing(String name) {return null}