/**
 * This script sends a SQL request for the current data from the HPCScheduler.Job database .
 * It counts all the Calls that are currently running and gives total stats
 * It checks to see if today's filename exists - if it does it will not overwrite the data. If it doesn't exist, it will create it!
 * Takes SQL location from SQLconfig.properties or other file specified in argument
 * User: Ben Newton
 * Date: 18/03/2015
 * Time: 09:11
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

def sqlConnect ="jdbc:sqlserver://$host;instanceName=$instance;DatabaseName=HPCScheduler;integratedSecurity=$security"

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


def getHPCLiveCallIndividuals(sql){

    query="SELECT CURRENT_TIMESTAMP AS 'Timestamp', ID, ISNULL(NumberOfCalls,0) AS TotalCalls,ISNULL(NumOfOutstandCalls,0) AS QueuedCalls,ISNULL(NumberOfCalls - NumOfOutstandCalls,0) AS CompletedCalls, CallsPerSecond,CallDuration,Progress, ISNULL(DATEDIFF(MS,CreateTime,SubmitTime),0) AS CR_SUB_MS, ISNULL(DATEDIFF(MS,SubmitTime,StartTime),0) AS SUB_STA_MS, ISNULL(DATEDIFF(S,CreateTime,GETDATE()),0) AS TotalElapsedSecs, ISNULL(DATEDIFF(S,ChangeTime,GetDate()),0) AS ChangeElapsedSecs FROM dbo.Job WHERE State < 128 ORDER BY ID"


    sql.eachRow(query){   row ->

		println ("$row.Timestamp,$row.ID,$row.TotalCalls,$row.QueuedCalls,$row.CompletedCalls,$row.CallsPerSecond,$row.CallDuration,$row.Progress,$row.CR_SUB_MS,$row.SUB_STA_MS,$row.TotalElapsedSecs,$row.ChangeElapsedSecs")
    }
}


try{
    def sql= new Sql(conn)
    getHPCLiveCallIndividuals(sql)
}

catch(Exception e){

    perr << e

}