package com.thinkbig.tomcat.api.impl;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.thinkbig.tomcat.api.model.ReadHiveConfiguration;
import com.thinkbig.tomcat.mapreduce.ReadHiveMapper;

/**
 * Job that reads sql commands from input files, executes those sql commands against a Hive server.
 * The output is a text file with the sql command, followed by the result of the query.
 * @author Think Big Analytics
 *
 */
public class ReadHiveJobLauncherService extends AbstractJobLauncherService<ReadHiveConfiguration>
{
	
	@Override
	protected void configureJob(Job job, ReadHiveConfiguration jobConfiguration) throws IOException, InterruptedException
	{
		Configuration config = job.getConfiguration();
		
		config.set(ReadHiveMapper.HIVE_URL, jobConfiguration.getHiveUrl());
		config.set(ReadHiveMapper.HIVE_USER, jobConfiguration.getHiveUser());
		config.set(ReadHiveMapper.HIVE_PASSWORD, jobConfiguration.getHivePassword());
		
		FileInputFormat.addInputPath(job, new Path(jobConfiguration.getInputDirectory()));
		FileOutputFormat.setOutputPath(job, new Path(jobConfiguration.getOutputDirectory()));
		
		job.setMapperClass(ReadHiveMapper.class);
		job.setNumReduceTasks(0);
		
	}
	
	
	
	@Override
	public boolean runJob(Job job, ReadHiveConfiguration jobConfiguration) throws IOException, InterruptedException
	{
		try
		{
			Connection connection = DriverManager.getConnection(jobConfiguration.getHiveUrl());
			Path input = new Path(jobConfiguration.getInputDirectory());
			FileSystem fileSystem = input.getFileSystem(job.getConfiguration());
			
		}
		catch (SQLException e)
		{
			throw new IOException("Error executing sql commands", e);
		}
		
		return super.launchJob(jobConfiguration);
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
