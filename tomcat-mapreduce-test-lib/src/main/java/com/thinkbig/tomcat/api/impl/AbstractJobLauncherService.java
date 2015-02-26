package com.thinkbig.tomcat.api.impl;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.Logger;

import com.thinkbig.tomcat.api.JobLauncherService;

public abstract class AbstractJobLauncherService<T> implements JobLauncherService<T>
{

	// instance-level logger (for inheritance)
	protected final Logger logger = Logger.getLogger(getClass());
	
	
	public boolean launchJob(final T jobConfiguration) throws IOException, InterruptedException
	{
		final Job job = createJob();
		
		configureJob(job, jobConfiguration);
		
		return runJob(job, jobConfiguration);
	}
	
	protected abstract void configureJob(Job job, T jobConfiguration) throws IOException, InterruptedException;
	
	public boolean runJob(Job job, T jobConfiguration) throws IOException, InterruptedException
	{
		boolean success = false;
		
		try
		{
			job.waitForCompletion(true);
			success = job.isSuccessful();
			
			logger.warn("job.success: " + success);
			logger.warn("job.user: " + job.getUser());
		}
		catch (ClassNotFoundException e)
		{
			success = false;
			logger.warn("error running mapreduce job", e);
		}
		
		return success;
	}
	
	public Job createJob() throws IOException, InterruptedException
	{
		return createJob(createConfiguration());
	}
	
	public Job createJob(Configuration config) throws IOException, InterruptedException
	{
		final String localhost = InetAddress.getLocalHost().getHostName();
		final String principal = "bdp@CDH.PREPROD.WUDIP.COM";
		final String keytab = "/var/run/wufiles/bdp/bdp.keytab";
		
		final boolean kerberos = "kerberos".equals(config.get("hbase.security.authentication"));
		
		config.set("hbase.myclient.keytab", keytab);
		config.set("hbase.myclient.principal", principal);
		
		if (kerberos)
		{
			// this is essentially a kinit: (apparently)
			logger.info("kerberos authentication method found in config");
			UserGroupInformation.setConfiguration(config);
			UserGroupInformation.loginUserFromKeytab(principal, keytab);
			User.login(config, "hbase.myclient.keytab", "hbase.myclient.principal", localhost);
		}
		
		final Job job = Job.getInstance(config);
		
		if (kerberos) 
		{
			User.getCurrent().obtainAuthTokenForJob(config, job);
		}
		
		logger.warn("job.user: " + job.getUser());
		logger.warn("user.name: " + System.getProperty("user.name"));
		
		return job;
	}
	
	public Configuration createConfiguration() throws IOException
	{
		Configuration config = new YarnConfiguration(new Configuration());
		
		config.addResource("core-site.xml");
		config.addResource("hdfs-site.xml");
		config.addResource("mapred-site.xml");
		config.addResource("yarn-site.xml");
		
		config = HBaseConfiguration.create(config);
		
//		PrintWriter w = new PrintWriter(System.out);
//		Configuration.dumpConfiguration(config, w);
		logger.warn("mapreduce.framework.name " + config.get("mapreduce.framework.name"));
		logger.warn("yarn.resourcemanager.address " + config.get("yarn.resourcemanager.address"));
		logger.warn("yarn.resourcemanager.scheduler.address " + config.get("yarn.resourcemanager.scheduler.address"));
		logger.warn("config: " + config);
		
		return config;
	}
}
