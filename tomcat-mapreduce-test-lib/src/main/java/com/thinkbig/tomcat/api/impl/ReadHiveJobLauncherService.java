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
		int index = 0;
		final String hiveUrl = args[index++];
		final String hiveUser = args[index++];
		final String hivePassword = args[index++];
		final String inputDirectory = args[index++];
		final String outputDirectory = args[index++];
		
		
		ReadHiveJobLauncherService service = new ReadHiveJobLauncherService();
		ReadHiveConfiguration config = new ReadHiveConfiguration(hiveUrl, hiveUser, hivePassword, inputDirectory, outputDirectory);
		
		service.launchJob(config);
		
	}
	

}
