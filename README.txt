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

Read/Write for Hive as follows:
~> hadoop jar uber-tomcat-mapreduce-test-lib-0.0.1-SNAPSHOT.jar com.thinkbig.tomcat.api.impl.ReadHiveJobLauncherService jdbc:hive2://localhost:10000 cloudera cloudera inputDirectory outputDirectory

Hive tests:
	The Hive access test expects a simple text file with 1 hive query per line
	an example file might look like this:

show tables
select * from some_existing_table
create table some_other_table as select * from some_existing_table
select * from some_other_table
drop table some_other_table
show tables

	The Hive connection is made locally first (before the job is submitted) to ensure connectivity to the Hive server can be 
	made by an edge node.  Then the job is submitted and a single Mapper get's the input for this file 
	(note that the size of the input file should be small to ensure only a single Mapper is used)

	The output of the Hive test is simple text, with the query executed, followed by the output of the query
	A query that has multiple result records will have multiple lines output in the target file (1 per ResultSet record)
For web-app based access:
	- deploy the tomcat-mapreduce-test-war-0.0.1-SNAPSHOT.war to a local tomcat server
	- access the web app from it's context root, for example:
		http://localhost:8080/myapp/jobs/DefaultJobLauncherService?inputDirectory=input&outputDirectory=output
	- the URI must end with the name of the service being launched, (one of the service names above)
	- the following URL parameters possible:
		- inputDirectory
		- outputDirectory
		- tableName
		- columnFamily
		- hiveUrl (hive tests only)
		-hiveUser (hive tests only)
		-hivePassword (hive tests only)

	


