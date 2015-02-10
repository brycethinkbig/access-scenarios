package com.thinkbig.tomcat.api.impl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.thinkbig.tomcat.api.model.WriteHBaseConfig;
import com.thinkbig.tomcat.mapreduce.WriteHBaseMapper;

public class WriteHBaseJobLauncherService extends AbstractJobLauncherService<WriteHBaseConfig>
{
	
	@Override
	protected void configureJob(Job job, WriteHBaseConfig jobConfiguration) throws IOException, InterruptedException
	{
		final Configuration config = job.getConfiguration();
		
		Path inputDirectory = new Path(jobConfiguration.getInputDirectory());
		FileInputFormat.addInputPath(job, inputDirectory);
		
		config.set(WriteHBaseMapper.OUTPUT_TABLENAME, jobConfiguration.getTablename());
		config.set(WriteHBaseMapper.OUTPUT_COLUMN_FAMILY, jobConfiguration.getColumnFamily());
		
		job.setJarByClass(WriteHBaseMapper.class);
		
		job.setMapperClass(WriteHBaseMapper.class);
		// no reducing here:
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(NullOutputFormat.class);
		
	}
	
	public static void main(String[] args) throws IOException, InterruptedException
	{
		final String inputDirectory = args[0];
		final String tablename = args[1];
		final String columnFamily = args[2];
		
		WriteHBaseConfig config = new WriteHBaseConfig(inputDirectory, tablename, columnFamily);
		WriteHBaseJobLauncherService service = new WriteHBaseJobLauncherService();
		service.launchJob(config);
		
	}
	
}
