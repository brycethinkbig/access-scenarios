package com.thinkbig.tomcat.api.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.thinkbig.tomcat.api.model.ReadHiveConfiguration;
import com.thinkbig.tomcat.mapreduce.ReadHiveMapper;
import com.thinkbig.tomcat.util.NullSafe;

/**
 * Job that reads sql commands from input files, executes those sql commands against a Hive server.
 * The output is a text file with the sql command, followed by the result of the query.
 * @author Think Big Analytics
 *
 */
public class ReadHiveJobLauncherService extends AbstractJobLauncherService<ReadHiveConfiguration>
{
	private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
	
	@Override
	protected void configureJob(Job job, ReadHiveConfiguration jobConfiguration) throws IOException, InterruptedException
	{
		try
		{
			Class.forName(driverName);
		}
		catch (ClassNotFoundException e)
		{
			throw new IllegalStateException("unable to load jdbc driver: " + driverName);
		}
		
		Configuration config = job.getConfiguration();
		
		config.set(ReadHiveMapper.HIVE_URL, jobConfiguration.getHiveUrl());
		config.set(ReadHiveMapper.HIVE_USER, jobConfiguration.getHiveUser());
		config.set(ReadHiveMapper.HIVE_PASSWORD, jobConfiguration.getHivePassword());
		
		FileInputFormat.addInputPath(job, new Path(jobConfiguration.getInputDirectory()));
		FileOutputFormat.setOutputPath(job, new Path(jobConfiguration.getOutputDirectory()));
		
		job.setMapperClass(ReadHiveMapper.class);
		job.setNumReduceTasks(0);
		job.setJarByClass(ReadHiveMapper.class);
		
	}
	
	@Override
	public boolean runJob(Job job, ReadHiveConfiguration jobConfiguration) throws IOException, InterruptedException
	{
		Connection connection = null;
		try
		{
			if (!NullSafe.isEmpty(jobConfiguration.getHiveUser()) && !NullSafe.isEmpty(jobConfiguration.getHivePassword()))
			{
				connection = DriverManager.getConnection(
												jobConfiguration.getHiveUrl(), 
												jobConfiguration.getHiveUser(), 
												jobConfiguration.getHivePassword()
											);
			}
			else
			{
				connection = DriverManager.getConnection(jobConfiguration.getHiveUrl());
			}
			
			Path input = new Path(jobConfiguration.getInputDirectory());
			FileSystem fileSystem = input.getFileSystem(job.getConfiguration());
			
			FileStatus[] files = fileSystem.listStatus(input);
			if (!NullSafe.isEmpty(files))
			{
				FileStatus status = files[0];
				Path file = status.getPath();
				FSDataInputStream in = fileSystem.open(file);
				BufferedReader reader = new BufferedReader(new InputStreamReader(in));
				
				System.out.println("about to read file... " + file.getName());
				
				String line = reader.readLine();
				while (!NullSafe.isEmpty(line))
				{
					Statement statement = connection.createStatement();
					
					System.out.println("locally executing statement.execute('" + line + "')...");
					boolean hasResult = statement.execute(line);
					System.out.println("hasResult: " + hasResult);
					
					statement.close();
					line = reader.readLine();
				}
					
				System.out.println("done reading file... " + file.getName());
			}
			
		}
		catch (SQLException e)
		{
			throw new IOException("Error executing sql commands", e);
		}
		finally
		{
			if (connection != null)
			{
				try
				{
					connection.close();
				}
				catch (SQLException e)
				{
					throw new IOException("error closing connection", e);
				}
			}
		}
		
		return super.runJob(job, jobConfiguration);
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
