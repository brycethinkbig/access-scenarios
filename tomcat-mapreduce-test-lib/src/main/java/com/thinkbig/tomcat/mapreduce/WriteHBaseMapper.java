package com.thinkbig.tomcat.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.thinkbig.tomcat.hbase.HBaseConnection;
import com.thinkbig.tomcat.util.NullSafe;

public class WriteHBaseMapper extends Mapper<LongWritable, Text, Text, Text>
{
	
	public static final String OUTPUT_TABLENAME = WriteHBaseMapper.class.getSimpleName() + ".tableName";
	public static final String OUTPUT_COLUMN_FAMILY = WriteHBaseMapper.class.getSimpleName() + ".columnFamily";
	
	private HBaseConnection hbaseConnection;
	private HTableInterface table;
	
	private String tableName = "file_lines";
	private byte[] columnFamily = Bytes.toBytes("lines");
	private static final byte[] QUALIFIER = Bytes.toBytes("value");
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		super.setup(context);
		
		final Configuration config = context.getConfiguration();
		
		HConnection connection = HConnectionManager.createConnection(config);
		hbaseConnection = new HBaseConnection(connection);
		
		final String configTableName = config.get(OUTPUT_TABLENAME);
		final String configColumnFamily = config.get(OUTPUT_COLUMN_FAMILY);
		
		if (!NullSafe.isEmpty(configTableName)) {
			tableName = configTableName;
			columnFamily = Bytes.toBytes(configColumnFamily);
		}
		
		table = hbaseConnection.getTable(tableName);
		if (!hbaseConnection.tableExists(tableName)) {
			hbaseConnection.createTable(tableName, configColumnFamily);
		}
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		super.cleanup(context);
		hbaseConnection.close();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		byte[] rowKey = Bytes.toBytes(key.get());
		Put put = new Put(rowKey);
		put = put.add(columnFamily, QUALIFIER, value.copyBytes());
		
		table.put(put);
	}
	
}
