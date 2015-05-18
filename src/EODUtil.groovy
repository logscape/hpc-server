/**
 * This script sends a SQL request for yesterday's usage from the HPC ReportingView.DailyNodeStatView database .
 * It counts all the figures for the Jobs ended yesterday and gives total stats
 * It checks to see if today's filename exists - if it does it will not overwrite the data. If it doesn't exist, it will create *it!
 * User: Ben Newton
 * Date: 20/10/14
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


def getEODUtil(sql){

    query="SELECT [Date],N.NodeName,N.NumberOfCores, V.UtilizedTime AS TotalUtilised, V.UtilizedTime / N.NumberOfCores as UtilisedByCore,V.CoreAvailableTime AS TotalAvailable, V.CoreAvailableTime / N.NumberOfCores AS AvailableByCore,V.CoreTotalTime, V.CoreTotalTime / N.NumberOfCores AS TotalByCore FROM HpcReportingView.DailyNodeStatView V INNER JOIN Node N ON N.NodeName = V.NodeName WHERE CONVERT(DATE,[Date]) = CONVERT(DATE,GETDATE()-1) AND N.NumberOfCores IS NOT NULL"


    sql.eachRow(query){   row ->

		println ("$row.Date,$row.NodeName,$row.NumberofCores,$row.TotalUtilised,$row.UtilisedByCore,$row.TotalAvailable,$row.AvailableBycore,$row.CoreTotalTime,$row.TotalByCore")
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
    	if (file.path.contains("EODUtil.out")){
		list << file.path
	}
	}

	if (list == []){
	
	def sql= new Sql(conn)
    getEODUtil(sql)
	}
	else{
	pass
}
}
catch(Exception e){

    perr << e

}