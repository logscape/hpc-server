/**
 * This script sends a SQL request for yesterday's stats from the HPCScheduler.Job database .
 * It counts all the figures for the Jobs ended yesterday and gives total stats
 * It checks to see if today's filename exists - if it does it will not overwrite the data. If it doesn't exist, it will create it!
 * You can specify a different SQL properties file at runtime using it as an argument
 * User: Ben Newton
 * Date: 18/03/2015
 * Time: 13:10
 * 
 */

import java.sql.Connection
import java.sql.DriverManager
import javax.sql.DataSource
import groovy.sql.Sql
import java.lang.reflect.Field

System.setProperty("java.library.path", "./lib");
 Field fieldSysPath = ClassLoader.class.getDeclaredField( "sys_paths" );
fieldSysPath.setAccessible( true );
fieldSysPath.set( null, null );

Properties properties = new Properties()
def props = args.length > 0 ? ".\\lib\\" + args[0] : ".\\lib\\SQLconfig.properties"
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

def host = properties.dbHost
def instance = properties.dbInstance
def security = properties.security
def user = properties.user
def pwd = properties.password


pout = pout == null ? System.out : pout
perr = perr == null ? System.err : perr

dbDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver" 

def sqlConnect ="jdbc:sqlserver://$host;instanceName=$instance;DatabaseName=HPCReporting;integratedSecurity=$security"

conn= sqlConnect


def propertyMissing (String name) {}

def log( line ){
    dt = new Date()
    timestamp = String.format('%1$row.te-%1$row.tb-%1$row.ty %tT',dt)
    pout <<  "" + timestamp + "\t" + line  + "\n"
}


def stdOut = pout
def stdErr = perr


def driver = Class.forName(dbDriver).newInstance();


def conn = DriverManager.getConnection(conn,user,pwd)


def getEODStats(sql){

    query="SELECT CONVERT(DATE,GETDATE()-1) AS TimeStamp, ServiceName,COUNT(DISTINCT J.ID) AS Jobs,SUM(CASE WHEN t.instanceID <0 THEN NumberOfCalls ELSE 0 END) as Calls,SUM(CASE WHEN t.instanceID <0 THEN(DATEDIFF(SECOND,J.StartTime,J.EndTime)) ELSE 0 END) as JobSeconds,SUM(CASE WHEN t.instanceID <0 THEN(DATEDIFF(Minute,J.StartTime,J.EndTime)) ELSE 0 END) as JobMins,SUM(CASE WHEN t.instanceID <0 THEN CallDuration ELSE 0 END) as Duration , SUM(DATEDIFF(SECOND,T.StartTime,T. EndTime)) AS TaskSecs, SUM(DATEDIFF(MINUTE,T.StartTime,T. EndTime)) AS TaskMins FROM dbo.Job J INNER JOIN dbo.Task T ON J.ID = T.ParentJobId WHERE ServiceName IS NOT NULL AND CONVERT(DATE,J.EndTime) = CONVERT(DATE,GETDATE()-1)GROUP BY ServiceName"
    
    sql.eachRow(query){   row ->

		println ("$row.Timestamp 00:00:00,$row.ServiceName,$row.Jobs,$row.Calls,$row.JobSeconds,$row.JobMins,$row.Duration,$row.TaskSecs,$row.TaskMins")
    }
}


try{

	def list = []
	def today = new Date()
	def fname = today.format('yyMMMdd')
	def appdir = "..//..//work//MicrosoftHPCApp-1.1//"
	def path = appdir + fname
	def work = new File(path)
	def work2 = new File(appdir)

	if(!work2.exists()){
	work2.mkdir()
	}

	if(!work.exists()){
	work.mkdir()
	}
	
		work.eachFileRecurse {file ->
    	if (file.path.contains("EODStats.out")){
		list << file.path
	}
	}

	if (list == []){
	
	def sql= new Sql(conn)
    getEODStats(sql)
	}
	else{
	pass
}
}
catch(Exception e){

    perr << e

}