package com.thinkbig.tomcat.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReadHBaseReducer extends Reducer<Text, Text, Text, Text>
{
	
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
		// we just spit out what was given to us:
		for (Text value : values)
		{
			context.write(key, value);
		}
	}

}
