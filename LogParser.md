HPC Server LogParser
Ben.Newton@excelian.com
24/12/2014

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This program allows you to parse the logs provided by HPC Server 2012 SP1 or greater.
It was designed to integrate with Logscape, so it can be set to run automatically or manually depending on depth and space requirements. 

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# ***System Requirements***:

 - HPC Server 2012 SP 1 or greater must be run on the machine (needs the hpctrace command)
 - Java 1.6 or greater installed and configured

 
# ***File List***:
 - logparser.groovy = The main script
 - parserRunner.bat = A batch file that will execute it manually. 
 - lib/parser.properties = The file used by default, manual operation. Will need configuring for paths
 - lib/parser.config = Logscape configuration for the Data Type etc.
 - BATCHparser.properties = A n example of a batch style of Properties file, will need configuring for paths. 
 

#Initial Setup 

Before running for the first time, you'll need to configure at least one properties file - found in the lib folder.
This file controls the actions of the script as well as directory paths. By default, it is set to manual mode: so you use parserRunner.bat to go through a menu and choose which logs to extract. 

# Batch Running

If you wish to use a batch style of logging and run it on a schedule, you may need to use more than one properties file: for example you might want detailed logging on the Broker Nodes but only severe errors on the Worker Nodes.

You can select which Properties file to use by executing the groovy script with an argument when including it in the bundle file.

eg <script>logparser.groovy "BATCHparser.properties</script>"

Note that the mode selection in the properties file used must be set to 0 (batch mode) otherwise it will start the manual process. 

You will need to add the service to the .bundle file - see EXAMPLE_bundle_with_Log_Parser.txt to see how best to do this.

IT IS RECOMMENDED THAT YOU LEAVE parser.properties FOR MANUAL OPERATION AND USE BATCHparser.properties FOR USE IN A LOGSCAPE SERVICE.


# Modes and Options

runMode [0,1]

Manual Mode. The script guides you through, asking which logs you'd like parsed and to what level. 
Set to runMode 1. 

Automatic Mode. The script requires no user interaction, it uses the settings pre-configured in the properties file.
Set to runMode 0.

	This determines whether it is going to run automatically using the defaults in the file [0] or whether you wish to run it manually and select your requirements as you go [1]. If you're running it as a regular script, set it to 0. 1 is designed for on the fly trouble shooting.

sourcePath [directory] - Where the logs are going to be found. Needs to point to the LogFiles directory of HPC. Please note, you must use forward slashes / instead of backwards otherwise you'll get some interesting errors.

destinationPath [directory] - Where the extracted files should be sent to. Same rules as before regarding /

## storage options  [0,1,2] 
 Determines how you wish to store the files.

	0 = No Storage. The destination directory is wiped of ALL log files (but not folders or their contents) and the latest files are put in. Do NOT make destinationPath a sensistive directory if you are using this option. -- Designed for debugging and pulling data quickly.
	
	1 = Once per Day. A subfolder is created based on the date and files are extracted there. If you try and run this again during batch, it will refuse. Designed for batching
	
	2 = Every Run. A subfolder is created for every run.

## Select Default Level
 defaultLevel = [1,2,3,4] Determines the default level of parsing - that which will be used in run mode 0. 

	1 Error and Critical only
	
	2 Warning, Error, and Critical only
	
	3 Info, Warning, Error and Critical only
	
	4 All: Verbose, Info, Warning, Error, and Critical
 
## Choose Method
 choiceMethod [1,2] - This determines whether you get all logs of a certain type, or a selection based on age.

	1. Type Selection. The simplest. Pick a type of log and they will all be parsed for you. If you have a lot of logs, this could take a while.
	
	2. Time Selection. Select the type and the age, the script will only parse the ones that meet those criteria. 

## Days 
days [0,1,2,3...] Any number. If using method 2, this is the age in days it will look for. 

## Option Select 1 
 select1 [mgmt] If using method 1, which files it will search for by default. The choices are:

["1":"mgmt","2":"sche","3":"sdm","4":"diag","5":"rept","6":"nmgr","7":"msvrmbrok","8":"mclt","9":"brok","10":"sdgm","11":"sess"]

		1.  HPC Management Service"
		2.  HPC Job Scheduler Service"
		3.  HPC SDM Store Service"
		4.  HPC Diagnostics Service"
		5.  HPC Reporting Service"
		6.  HPC Node Manager Service"	 
		7.  HPC Monitoring Server Service"	 
		8.  HPC Monitoring Client Service"
		9.  HPC Broker Service"
		10. HPC SOA Diag Mon Service"
		11. HPC Session Service"

## Option Select 2 
 select2 [.bin] - The pattern to search for if using dates. .bin will get everything.
