This project has a number of tests that run through access scenarios for the core project

There are 2 means of launching these scenarios:
	1. via command line
	2. via REST call through a tomcat server

Test Scenarios:

Vanilla word count:

~> hadoop jar uber-tomcat-mapreduce-test-lib-0.0.1-SNAPSHOT.jar com.thinkbig.tomcat.api.impl.DefaultJobLauncherService inputDirectory outputDirectory

 

~> hadoop jar uber-tomcat-mapreduce-test-lib-0.0.1-SNAPSHOT.jar com.thinkbig.tomcat.api.impl.ReadHBaseJobLauncherService tableName columnFamilyName outputDirectory

~> hadoop jar uber-tomcat-mapreduce-test-lib-0.0.1-SNAPSHOT.jar com.thinkbig.tomcat.api.impl.RandomReadHBaseJobLauncherService tableName columnFamilyName inputDirectory outputDirectory

~> hadoop jar uber-tomcat-mapreduce-test-lib-0.0.1-SNAPSHOT.jar com.thinkbig.tomcat.api.impl.WriteHBaseJobLauncherService inputDirectory tableName columnFamilyName
