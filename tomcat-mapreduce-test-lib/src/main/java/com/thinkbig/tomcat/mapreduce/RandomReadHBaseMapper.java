package com.thinkbig.tomcat.mapreduce;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.thinkbig.tomcat.hbase.HBaseConnection;
import com.thinkbig.tomcat.util.NullSafe;

/**
 * Silly mapper that just counts the number of rows in HBase that are identified by keys
 * that match lines of input from the source of the MapReduce job.
 * @author Think Big Analytics
 *
 */
public class RandomReadHBaseMapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
	
	public static final String TABLE_NAME = RandomReadHBaseMapper.class.getName() + ".tableName";
	public static final String COLUMN_FAMILY = RandomReadHBaseMapper.class.getName() + ".columnFamily";
	
	private HBaseConnection hbaseConnection;
	private HTableInterface table;
	private final LongWritable ONE = new LongWritable(1);
	
	private byte[] columnFamily = null;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		super.setup(context);
		
		HConnection connection = HConnectionManager.createConnection(context.getConfiguration());
		hbaseConnection = new HBaseConnection(connection);
		
		final Configuration config = context.getConfiguration();
		final String configName = config.get(TABLE_NAME);
		if (NullSafe.isEmpty(configName))
		{
			throw new IllegalStateException(TABLE_NAME + " is empty");
		}
		
		table = hbaseConnection.getTable(configName);
		final String family = config.get(COLUMN_FAMILY);
		
		if (NullSafe.isEmpty(family))
		{
			throw new IllegalStateException(COLUMN_FAMILY + " is empty");
		}
		
		columnFamily = Bytes.toBytes(family);
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		super.cleanup(context);
		table.close();
		hbaseConnection.close();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		if (value.getLength() > 0)
		{
			final Get get = new Get(value.copyBytes());
			Result result = table.get(get);
			
			if (!result.isEmpty())
			{
				NavigableMap<byte[], byte[]> row = result.getFamilyMap(columnFamily);
				
				if (!NullSafe.isEmpty(row))
				{
					context.write(value, ONE);
				}
			}
		}
		
	}
	
}
