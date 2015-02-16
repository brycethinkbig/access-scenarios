package com.thinkbig.tomcat.api.impl;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

import com.thinkbig.tomcat.api.model.LocalReadHBaseConfig;
import com.thinkbig.tomcat.hbase.HBaseConnection;

public class LocalReadHBaseJobLauncherService extends AbstractJobLauncherService<LocalReadHBaseConfig>
{

	@Override
	protected void configureJob(Job job, LocalReadHBaseConfig jobConfiguration) throws IOException, InterruptedException
	{
		// this is a local-only job, so... nothing to do here...
	}
	
	@Override
	public boolean runJob(Job job, LocalReadHBaseConfig jobConfiguration) throws IOException, InterruptedException
	{
		
		HConnection connection = HConnectionManager.createConnection(job.getConfiguration());
		HBaseConnection hbase = new HBaseConnection(connection);
		
		try
		{
			final byte[] columnFamily = Bytes.toBytes(jobConfiguration.getColumnFamily());
			
			// first, clean up the table if it's already there:
			final String newTableName = "new_" + jobConfiguration.getTablename();
			if (hbase.tableExists(newTableName))
			{
				hbase.deleteTable(newTableName);
			}
			
			// now create it:
			hbase.createTable(newTableName, jobConfiguration.getColumnFamily());
			
			// now scan through the old one and push all the results into the new one:
			HTableInterface source = hbase.getTable(jobConfiguration.getTablename());
			HTableInterface target = hbase.getTable(newTableName);
			ResultScanner scanner = source.getScanner(columnFamily);
			
			Result result = null;
			while ((result = scanner.next()) != null)
			{
				Put put = new Put(result.getRow());
				NavigableMap<byte[], byte[]> map = result.getFamilyMap(columnFamily);
				
				for (byte[] qualifier : map.keySet())
				{
					byte[] value = map.get(qualifier);
					put.add(columnFamily, qualifier, value);
				}
				
				target.put(put);
			}
		}
		finally
		{
			hbase.close();
		}
		
		return true;
	}
	
	public static void main(String[] args) throws IOException, InterruptedException
	{
		int index = 0;
		final String tablename = args[index++];
		final String columnFamily = args[index++];
		
		LocalReadHBaseConfig config = new LocalReadHBaseConfig(tablename, columnFamily);
		LocalReadHBaseJobLauncherService service = new LocalReadHBaseJobLauncherService();
		
		service.launchJob(config);
	}
	
	
}
