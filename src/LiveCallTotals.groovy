/**
 * This script sends a SQL request for the current data from the HPCScheduler.Job database .
 * It counts all the Calls that are currently running and gives total stats
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
 
def sqlConnect ="jdbc:sqlserver://$host;instanceName=$instance;DatabaseName=HPCScheduler;integratedSecurity=$security"

dbDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver" 

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


	def getHPCLiveCallTotals(sql){

    query="SELECT CURRENT_TIMESTAMP AS 'Timestamp',ISNULL(SUM(NumberOfCalls),0) AS TotalCalls,ISNULL(SUM(NumOfOutstandCalls),0) AS OutstandingCalls, (SELECT ISNULL(SUM(TaskRunning),0) from NodeResourceCounter) as RunningCalls, ISNULL(SUM(NumberOfCalls- NumOfOutstandCalls),0) AS CompletedCalls, ISNULL(AVG(CallDuration),0) AS AvgDuration FROM dbo.Job WHERE NumOfOutstandCalls > 0 AND State < 128 "


    sql.eachRow(query){   row ->

		println ("$row.Timestamp,$row.TotalCalls,$row.OutstandingCalls,$row.RunningCalls,$row.CompletedCalls,$row.AvgDuration")
    }
}

try{
    def sql= new Sql(conn)
    getHPCLiveCallTotals(sql)
}

catch(Exception e){

    perr << e

}