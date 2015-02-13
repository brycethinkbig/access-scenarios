package com.thinkbig.tomcat.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class ReadHBaseReducer extends Reducer<Text, Text, Text, Text>
{
	protected final Logger logger = Logger.getLogger(getClass());
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		super.cleanup(context);
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		super.setup(context);
	}
	
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		logger.info("Starting reducer...");
		// we just spit out what was given to us:
		for (Text value : values)
		{
			context.write(key, value);
		}
	}

}
