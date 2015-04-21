package com.thinkbig.tomcat.api.impl;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.thinkbig.tomcat.api.model.TomcatJobConfiguration;
import com.thinkbig.tomcat.mapreduce.WordCountMapper;
import com.thinkbig.tomcat.mapreduce.WordCountReducer;

public class DefaultJobLauncherService extends AbstractJobLauncherService<TomcatJobConfiguration>
{
	
	@Inject
	public DefaultJobLauncherService()
	{
		
	}
	
	@Override
	protected void configureJob(final Job job, final TomcatJobConfiguration jobConfiguration) throws IOException, InterruptedException
	{
		FileInputFormat.addInputPath(job, new Path(jobConfiguration.getInputDirectory()));
		FileOutputFormat.setOutputPath(job, new Path(jobConfiguration.getOutputDirectory()));
		
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setJarByClass(WordCountMapper.class);
	}
	
	public static void main(String[] args) throws IOException, InterruptedException
	{
		String inputDirectory = args[0];
		String outputDirectory = args[1];
		
		System.out.println("inputDirectory: " + inputDirectory);
		System.out.println("outputDirectory: " + outputDirectory);
		
		TomcatJobConfiguration jobConf = new TomcatJobConfiguration(inputDirectory, outputDirectory);
		DefaultJobLauncherService service = new DefaultJobLauncherService();
		
		System.out.println("launching job");
		boolean success = service.launchJob(jobConf, false);
		System.out.println("job success: " + success);
	}

}
