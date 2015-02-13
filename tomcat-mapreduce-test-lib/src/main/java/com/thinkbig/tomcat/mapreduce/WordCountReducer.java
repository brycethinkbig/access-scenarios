package com.thinkbig.tomcat.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>
{
	protected final Logger logger = Logger.getLogger(getClass());
	private Text outputKey = new Text();
	private LongWritable outputValue = new LongWritable();

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
	{
		logger.info("Starting reducer...");
		long count = 0;
		
		for (LongWritable value : values)
		{
			count += value.get();
		}
		
		outputKey.set(key.toString());
		outputValue.set(count);
		
		context.write(outputKey, outputValue);
	}
	
}
