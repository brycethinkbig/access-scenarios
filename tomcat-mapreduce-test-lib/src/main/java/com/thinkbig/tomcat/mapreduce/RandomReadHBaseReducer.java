package com.thinkbig.tomcat.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class RandomReadHBaseReducer extends Reducer<Text, LongWritable, Text, LongWritable>
{
	protected final Logger logger = Logger.getLogger(getClass());
	private final LongWritable valueOut = new LongWritable(1);
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		logger.info("Starting setup");
		super.setup(context);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		super.cleanup(context);
	}

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
	{
		logger.info("Starting reducer...");
		long count = 0;
		
		for (LongWritable value : values)
		{
			count += value.get();
		}
		
		valueOut.set(count);
		context.write(key, valueOut);
		
	}
	
}
