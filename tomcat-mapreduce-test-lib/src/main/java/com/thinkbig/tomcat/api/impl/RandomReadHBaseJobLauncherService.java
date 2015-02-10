package com.thinkbig.tomcat.api.impl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.thinkbig.tomcat.api.model.RandomReadHBaseConfig;
import com.thinkbig.tomcat.mapreduce.RandomReadHBaseMapper;
import com.thinkbig.tomcat.mapreduce.RandomReadHBaseReducer;

public class RandomReadHBaseJobLauncherService extends AbstractJobLauncherService<RandomReadHBaseConfig>
{

	@Override
	protected void configureJob(Job job, RandomReadHBaseConfig jobConfiguration) throws IOException, InterruptedException
	{
		final Configuration config = job.getConfiguration();
		
		config.set(RandomReadHBaseMapper.TABLE_NAME, jobConfiguration.getTablename());
		config.set(RandomReadHBaseMapper.COLUMN_FAMILY, jobConfiguration.getColumnFamily());
		
		FileInputFormat.addInputPath(job, new Path(jobConfiguration.getInputDirectory()));
		FileOutputFormat.setOutputPath(job, new Path(jobConfiguration.getOutputDirectory()));
		
		job.setJarByClass(RandomReadHBaseMapper.class);
		
		job.setMapperClass(RandomReadHBaseMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setReducerClass(RandomReadHBaseReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
	}
	
	public static void main(String[] args) throws IOException, InterruptedException
	{
		final String tablename = args[0];
		final String columnFamily = args[1];
		final String inputDirectory = args[2];
		final String outputDirectory = args[3];
		
		RandomReadHBaseConfig config = new RandomReadHBaseConfig(tablename, columnFamily, inputDirectory, outputDirectory);
		RandomReadHBaseJobLauncherService service = new RandomReadHBaseJobLauncherService();
		service.launchJob(config);
		
	}
}
