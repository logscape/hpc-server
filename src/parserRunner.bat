@ECHO OFF
START "HPC Server Log Parser" "cmd /C java -cp ..\..\lib\groovy-all-1.8.7.jar groovy.lang.GroovyShell logparser.groovy"
