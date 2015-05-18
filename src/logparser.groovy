/*
############################################################# 
# HPC Log Parser
# Ben.Newton@excelian.com
# This App allows you to parse the logs found in HPC.
# 04/12/2014
#
#############################################################
*/

println("Starting")

Properties properties = new Properties()
def props = args.length > 0 ? ".\\lib\\" + args[0] : ".\\lib\\parser.properties"
def propertiesFile = new File(props)

if (propertiesFile.exists()){
	propertiesFile.withInputStream{
    	properties.load(it)
    	}
	}

else
	{
	println "No Properties File Present"
	println System.getProperty("user.dir")
	return 0
	}

def getInput(defaultValue){
	value = new InputStreamReader(System.in).readLine()
	if (value == ""){
		if (defaultValue !="")
		value = defaultValue
		else{
			print("Input Required\n")
			return getInput(defaultValue)
		}
	}
	return value
}

println (props)
String mode = properties.runMode

//Setting defaults (used in both modes)

String ch2Def = properties.defaultLevel
String ch1Def = properties.choiceMethod
File source = new File(properties.sourcePath)
File destination = new File(properties.destinationPath)
String logDays = properties.days
def level = ch2Def
def method = ch1Def
def filelist = []
def today = new Date()
def storage = properties.storage

//Manual Mode
if (mode=="1"){

	println "\nWelcome to the Microsoft HPC Server Log Parser"
	println "------------------------------------\n"
	println "This is set to Manual Mode - if you wish to run this as an automated script, please edit the parser.properties file and set the default settings you require"
	println "\nWould you like to parse all the available logs of a certain type or just a selection?"
	println "	1. All logs of a certain type"
	println "	2. A selection by type and date"
	println "\nPlease select a choice {1 or 2} and press enter.\n"

	method = getInput(ch1Def)

	println " You have selected $method\n\n"
	println " Please choose a Level\n"

	println"	1. Error and Critical only"
	println"	2. Warning, Error, and Critical only"
	println"	3. Info, Warning, Error and Critical only"
	println"	4. All: Verbose, Info, Warning, Error, and Critical"
	println" \nPlease select a choice {1-4} and press enter.\n"
		 
	level = getInput(ch2Def)

	println " You have selected $level\n"

	if (!source.exists()){
		println "The pre-configured source for the Logs is set to $source" 
		println "This directory does not exist. Please edit the parser.properties and try again.\n"
		println "Goodbye!\n"
		System.exit(1)
	}
	else{
		println "The pre-configured source for the Logs is set to $source"
		println "If you have another directory you wish to use, edit the parser.properties.\n\n"
	}

	if (!destination.exists()){
		println "The pre-configured destination for the Logs is set to $destination"
		println "This directory does not exist, it will be created for you."
		println "If you wish to change the directory, type N (case sensitive) to quit, then edit the properties file."
		println "If you are happy to have the directory created, press enter\n\n"
		
		def createit = getInput("Y")
		
		if (createit == "N"){
			println "Goodbye!"
			System.exit(1)
		}
		else{
			destination.mkdir()
			println "Destination file created"
		}
	}

	else{
		println "The pre-configured source for the Logs is set to $destination"
		println "If you have another directory you wish to use, edit the parser.properties.\n\n"
	}

	if (method=="1"){
		def providers = ["1":"mgmt","2":"sche","3":"sdm","4":"diag","5":"rept","6":"nmgr","7":"msvrmbrok","8":"mclt","9":"brok","10":"sdgm","11":"sess"]
			
			println"\n You have selected all logs of a certain type. Please select the type of Logs that you are interested in [1-11]."
			println"	----------------------------------"
			println"	1.  HPC Management Service"
			println"	2.  HPC Job Scheduler Service"
			println"	3.  HPC SDM Store Service"
			println"	4.  HPC Diagnostics Service"
			println"	5.  HPC Reporting Service"
			println"	6.  HPC Node Manager Service"	 
			println"	7.  HPC Monitoring Server Service"	 
			println"	8.  HPC Monitoring Client Service"
			println"	9.  HPC Broker Service"
			println"	10. HPC SOA Diag Mon Service"
			println"	11. HPC Session Service"
			println"	----------------------------------"
			println"	If you want ALL of them, do so by date"
 			
 			def selectP = getInput("")
 			selection = providers[selectP]

 			if (selection==null){
 				println "You have selected an invalid type."
				System.exit(2)
 			}

 			println" \nYou have selected $selectP" 
 		}


	if (method=="2"){
			
			def fileTypes = ["1":"Management","2":"Scheduler","3":"Sdm","4":"Diagnostics","5":"NodeManager","6":"MonitoringServer","7":"MonitoringClient","8":"Broker","9":"SoaDiagMon","10":"Session","11":".bin"]

			println"\nYou have chosen to parse logs by their date."
			println" Please select the Logs that you are interested in"
			println"	----------------------------------"
			println"	1.  HPC Management Service"
			println"	2.  HPC Job Scheduler Service"
			println"	3.  HPC SDM Store Service"
			println"	4.  HPC Diagnostics Service"
			println"	5.  HPC Node Manager Service"	 
			println"	6.  HPC Monitoring Server Service"	 
			println"	7.  HPC Monitoring Client Service"
			println"	8.  HPC Broker Service"
			println"	9.  HPC SOA Diag Mon Service"
			println"	10. HPC Session Service"
			println"	11. ALL OF THEM - This could take a while"
			println"	----------------------------------"
 			 			
 			def selectP = getInput("")
 			selection = fileTypes[selectP]

 			if (selection==null){
 				println "You have selected an invalid type."
				System.exit(2)
 			}

 			println" \nYou have selected $selectP" 
 		
			println"Please enter the number of days logging you would like"

			daysLogged = getInput(logDays)
			println"\n You have selected $daysLogged"
			def maxAge = today - daysLogged.toInteger()


			source.eachFileRecurse {file ->
				if(file.path.contains(selection)){
					def currentAge = new Date(file.lastModified()) 
					//println currentAge
						if(currentAge > maxAge){
						filelist << file
						}
					}
			}
	}
	
	println"How would you like to store this data?"
	println"	0. Short Term (One set of files)"
	println"		This will wipe all previous extracts to $destination"
	println"	1. Once a Day. A sub-folder will be created for today."
	println"	2. Every Run. A new folder will be created every time you run this."
	println"	-----------------------------------------------------------------------"
	storage = getInput(storage)
	println" You have selected $storage"
}

if(mode=="0"){

	if(!source.exists()){
		println "$source does not exist. Please edit the properties file"
		System.exit(1)
	}

	if(!destination.exists()){
		destination.mkdir()
	}

	if(method=="1"){
		selection = properties.select1
		println("Running all of Type $selection")
	}

	if(method=="2"){
	selection = properties.select2
				source.eachFileRecurse {file ->
				if(file.path.contains(selection)){
					def maxAge = today - logDays.toInteger()
					def currentAge = new Date(file.lastModified()) 
					//println currentAge
						if(currentAge > maxAge){
						filelist << file
						println(filelist)
						}
					}
				}
			}
}

//Storage Policy - 1, multiple or wipe clean
def fname 

if (storage=="1"){
	fname = today.format('yyMMMdd')
		destination.eachFile(){ file ->
		if(file.name.contains(fname))
			println "This has already run today."
			System.exit(3)
		}
}

if (storage=="2"){
	fname = today.format('yyMMMddHHmmss')
}

if (storage=="0"){
	fname = ""
	destination.eachFile{file ->
		if(file.name.contains(".log")){
		file.delete()
		}
	}
}

File newDest = new File(destination.toString() + "\\" + fname)
destination = newDest
if (!destination.exists()){
	destination.mkdir()
	println(destination)
}

//Running the commands with set parameters
def command
	
	if(method=="1"){
	command = """ cmd /C hpctrace getlog $selection $level -d:$destination"""
	println command
	def proc = command.execute()
	proc.waitFor()
	println "return code: ${ proc.exitValue()}"
	println "stderr: ${proc.err.text}"
	println "stdout: ${proc.in.text}" 
	}

	if(method=="2"){

		filelist.each(){ file ->
		command = """cmd /C hpctrace parselog \"$file\" $level -d:$destination"""
		println command
		def proc = command.execute()
		proc.waitFor()
		println "return code: ${ proc.exitValue()}"
		println "stderr: ${proc.err.text}"
		println "stdout: ${proc.in.text}" 
			}
	}
println("Exiting")
System.exit(0)