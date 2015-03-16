package com.thinkbig.tomcat.api.impl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import com.thinkbig.tomcat.api.model.WriteHBaseConfig;
import com.thinkbig.tomcat.hbase.HBaseConnection;
import com.thinkbig.tomcat.mapreduce.LongRunningMapper;

public class LongRunningJobLauncherService extends AbstractJobLauncherService<WriteHBaseConfig>
{
	
	@Override
	protected void configureJob(Job job, WriteHBaseConfig jobConfiguration) throws IOException, InterruptedException
	{
		final Configuration config = job.getConfiguration();
		
		Path inputDirectory = new Path(jobConfiguration.getInputDirectory());
		FileInputFormat.addInputPath(job, inputDirectory);
		
		config.set(LongRunningMapper.OUTPUT_TABLENAME, jobConfiguration.getTablename());
		config.set(LongRunningMapper.OUTPUT_COLUMN_FAMILY, jobConfiguration.getColumnFamily());
		
		job.setJarByClass(LongRunningMapper.class);
		
		job.setMapperClass(LongRunningMapper.class);
		// no reducing here:
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(NullOutputFormat.class);
		
	}
	
	@Override
	public boolean runJob(Job job, WriteHBaseConfig jobConfiguration) throws IOException, InterruptedException
	{
		// first, we setup the job:
		HConnection connection = HConnectionManager.createConnection(job.getConfiguration());
		HBaseConnection hbase = new HBaseConnection(connection);
		
		final Configuration conf = job.getConfiguration();
		final HTableInterface table = hbase.getTable(LongRunningMapper.COUNTER_TABLE_NAME);
		final Get get = new Get(LongRunningMapper.COUNTER_KEY);
		get.addColumn(LongRunningMapper.COLUMN_FAMILY, LongRunningMapper.COUNTER_QUALIFIER);
		
		try
		{
			if (!hbase.tableExists(LongRunningMapper.COUNTER_TABLE_NAME))
			{
				hbase.createTable(LongRunningMapper.COUNTER_TABLE_NAME, LongRunningMapper.COLUMN_FAMILY_NAME);
			}
			
			// now create it:
			
			// now scan through the old one and push all the results into the new one:
			Result result = table.get(get);
			if (result != null && !result.isEmpty())
			{
				// set the value to be 0:
				Put put = new Put(LongRunningMapper.COUNTER_KEY);
				put.add(LongRunningMapper.COLUMN_FAMILY, LongRunningMapper.COUNTER_QUALIFIER, Bytes.toBytes(0L));
				
				table.put(put);
			}
		}
		finally
		{
			hbase.close();
		}
		
		final UserGroupInformation user = UserGroupInformation.getCurrentUser();
		
		Runnable runnable = new Runnable()
		{
			public void run()
			{
				long count = 0;
				
				while (count < 10)
				{
					try
					{
						Result result = table.get(get);
						
						if (result != null && !result.isEmpty())
						{
							byte[] countBytes = result.getValue(LongRunningMapper.COLUMN_FAMILY, LongRunningMapper.COUNTER_QUALIFIER);
							count = countBytes == null ? 0 : Bytes.toLong(countBytes);
						}
						
						Thread.sleep(5L);
					}
					catch (Exception e)
					{
						throw new RuntimeException("Hit an exception trying to read hbase", e);
					}
					
				}
				
				// once we've broken out of our loop (i.e.: we've written 10 records)
				// destroy the tokens:
				invalidateTokens(user, conf);
			}
			
		};
		
		if (LOGOUT_FIRST)
		{
			// this allows us to test weather or not the invalidateTokens code actually invalidates tokens:
			invalidateTokens(user, conf);
		}
		else
		{
			// launch a thread to listen for records to be written:
//			new Thread(runnable).start();
		}
		
		return super.runJob(job, jobConfiguration);
	}
	
	private static void invalidateTokens(UserGroupInformation user, Configuration conf)
	{
		try
		{
			for (Token<? extends TokenIdentifier> token : user.getTokens())
			{
				token.cancel(conf);
			}
		}
		catch (Exception e)
		{
			throw new RuntimeException("error clearning tokens", e);
		}
	}
	
	// lame super-hack to get additional config in:
	private static volatile boolean LOGOUT_FIRST = false;
	
	public static void main(String[] args) throws IOException, InterruptedException
	{
		final String inputDirectory = args[0];
		final String tablename = args[1];
		final String columnFamily = args[2];
		
		if (args.length > 3)
		{
			// if we're here, then we have an "additional" flag which means to logout the user first:
			LOGOUT_FIRST = true;
		}
		
		WriteHBaseConfig config = new WriteHBaseConfig(inputDirectory, tablename, columnFamily);
		LongRunningJobLauncherService service = new LongRunningJobLauncherService();
		service.launchJob(config);
		
	}
	
}
