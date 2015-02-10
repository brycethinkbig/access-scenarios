This project has a number of tests that run through access scenarios for the core project

There are 2 means of launching these scenarios:
	1. via command line
	2. via REST call through a tomcat server

Test Scenarios:

Vanilla word count:
~> hadoop jar uber-tomcat-mapreduce-test-lib-0.0.1-SNAPSHOT.jar com.thinkbig.tomcat.api.impl.DefaultJobLauncherService inputDirectory outputDirectory

Read from HBase using TableInputFormat:
~> hadoop jar uber-tomcat-mapreduce-test-lib-0.0.1-SNAPSHOT.jar com.thinkbig.tomcat.api.impl.ReadHBaseJobLauncherService tableName columnFamilyName outputDirectory

Read from HBase using random Reads (HTableInterface) in the Mapper:
~> hadoop jar uber-tomcat-mapreduce-test-lib-0.0.1-SNAPSHOT.jar com.thinkbig.tomcat.api.impl.RandomReadHBaseJobLauncherService tableName columnFamilyName inputDirectory outputDirectory

Write to HBase using random writes in Reducer:
~> hadoop jar uber-tomcat-mapreduce-test-lib-0.0.1-SNAPSHOT.jar com.thinkbig.tomcat.api.impl.WriteHBaseJobLauncherService inputDirectory tableName columnFamilyName

For web-app based access:
	- deploy the tomcat-mapreduce-test-war-0.0.1-SNAPSHOT.war to a local tomcat server
	- access the web app from it's context root, for example:
		http://localhost:8080/myapp/jobs/DefaultJobLauncherService?inputDirectory=input&outputDirectory=output
	- the URI must end with the name of the service being launched, (one of the service names above)
	- there are 4 URL parameters possible:
		- inputDirectory
		- outputDirectory
		- tableName
		- columnFamily
	


