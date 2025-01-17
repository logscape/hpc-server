Quickly Getting Started with the Microsoft HPC App for Logscape.

Once you have unzipped the contents of the folder:

 1. Ensure your Logscape environment is operational. 

 2. Select the machine/s which will be running the various processes - generally the headnode.

 3. On the Logscape Agents page, use the filter to find that Headnode only (i.e. type contains HEADNODE)

 4. Open MicrosoftHPCApp-1.1.bundle and insert the filter into the resource selection string for all services. Leave all other parameters. 

 -- If you wish to use the HPC Log Parser, you must follow the separate setup instructions found in LogParserReadme.txt before continuing. The default bundle file does not include the HPCParser because it requires more setup. However, MicrosoftHPCApp-1.1.bundleEXAMPLE contains a bundle file with the HPC Parser correctly configured. 

 5. Open the config.properties file (found in the lib folder)

 6. Put the hostname and instance name of your SQL database in the relevant lines.
	The default assumes you use integrated security and that the Logscape user has permission to access. If need be, add user name and password too. 

 7. Save the files into a file named: MicrosoftHPCApp-1.1.zip

 8. Deploy to your Logscape Environment. 


For more on Bundle Files and Deployment, check the Logscape Documentation online. 
