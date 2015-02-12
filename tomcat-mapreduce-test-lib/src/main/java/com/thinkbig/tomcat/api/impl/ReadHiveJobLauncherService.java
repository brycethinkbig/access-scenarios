package com.thinkbig.tomcat.api.impl;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;

import com.thinkbig.tomcat.api.model.ReadHiveConfiguration;

public class ReadHiveJobLauncherService extends AbstractJobLauncherService<ReadHiveConfiguration>
{

	@Override
	protected void configureJob(Job job, ReadHiveConfiguration jobConfiguration) throws IOException, InterruptedException
	{
		
		
	}
	
	public static void main(String[] args) throws IOException, InterruptedException
	{
		
		ReadHiveJobLauncherService service = new ReadHiveJobLauncherService();
		ReadHiveConfiguration config = null;
		
		service.launchJob(config);
		
	}
	

}
