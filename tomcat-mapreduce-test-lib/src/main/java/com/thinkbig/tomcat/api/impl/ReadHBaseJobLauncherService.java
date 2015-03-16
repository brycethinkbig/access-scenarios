package com.thinkbig.tomcat.api.impl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.thinkbig.tomcat.api.model.ReadHBaseConfiguration;
import com.thinkbig.tomcat.mapreduce.ReadHBaseMapper;
import com.thinkbig.tomcat.mapreduce.ReadHBaseReducer;

public class ReadHBaseJobLauncherService extends AbstractJobLauncherService<ReadHBaseConfiguration>
{
	
	@Override
	protected void configureJob(Job job, ReadHBaseConfiguration jobConfiguration) throws IOException, InterruptedException
	{
		Configuration config = job.getConfiguration();
		
		final byte[] tablename = Bytes.toBytes(jobConfiguration.getTablename());
		
		// un-bounded scan:
		Scan scan = new Scan();
		scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, tablename);
		
		config.set(ReadHBaseMapper.COLUMN_FAMILY, jobConfiguration.getColumnFamily());
		
		Path outputDir = new Path(jobConfiguration.getOutputDirectory());
		FileOutputFormat.setOutputPath(job, outputDir);
		
		job.setMapperClass(ReadHBaseMapper.class);
		job.setReducerClass(ReadHBaseReducer.class);
		job.setJarByClass(ReadHBaseMapper.class);
		
		logger.warn("about to invoke TableMapReduceUtil.initTableMapperJob");
		TableMapReduceUtil.initTableMapperJob(
				tablename, 
				scan, 
				ReadHBaseMapper.class, 
				Text.class, 
				Text.class, 
				job,
				true
			);
		
		TableMapReduceUtil.addDependencyJars(job.getConfiguration(), Bytes.class);
		
	}
	
	public static void main(String[] args) throws IOException, InterruptedException
	{
		final String tablename = args[0];
		final String columnFamily = args[1];
		final String outputDirectory = args[2];
		
		ReadHBaseConfiguration config = new ReadHBaseConfiguration(tablename, columnFamily, outputDirectory);
		ReadHBaseJobLauncherService service = new ReadHBaseJobLauncherService();
		service.launchJob(config);
	}
	
}
