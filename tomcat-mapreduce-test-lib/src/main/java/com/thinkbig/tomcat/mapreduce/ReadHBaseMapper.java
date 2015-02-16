package com.thinkbig.tomcat.mapreduce;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.thinkbig.tomcat.util.NullSafe;

public class ReadHBaseMapper extends TableMapper<Text, Text>
{
	protected final Logger logger = Logger.getLogger(getClass());
	public static final String COLUMN_FAMILY = ReadHBaseMapper.class.getName() + ".columnFamily";
	
	private static final byte[] COLON = Bytes.toBytes(":");
	
	private final Text keyOut = new Text();
	private final Text valueOut = new Text();
	
	private byte[] familyName;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		logger.info("Starting setup");
		
		Configuration config = context.getConfiguration();
		
		final String familyString = config.get(COLUMN_FAMILY);
		
		if (NullSafe.isEmpty(familyString))
		{
			throw new IllegalStateException("familyName is missing");
		}
		
		familyName = Bytes.toBytes(familyString);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		super.cleanup(context);
	}
	
	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException
	{
		logger.info("Starting mapper...");
		keyOut.set(value.getRow());
		
		NavigableMap<byte[], byte[]> map = value.getFamilyMap(familyName);
		for (byte[] qualifier : map.keySet())
		{
			final byte[] qualifierValue = map.get(qualifier);
			valueOut.set(qualifier);
			valueOut.append(COLON, 0, COLON.length);
			valueOut.append(qualifierValue, 0, qualifierValue.length);
			
			context.write(keyOut, valueOut);
		}
		
	}
	
}
