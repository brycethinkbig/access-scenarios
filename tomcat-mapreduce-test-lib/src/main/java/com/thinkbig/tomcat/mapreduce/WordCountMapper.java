package com.thinkbig.tomcat.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
	
	private final Text outputKey = new Text();
	private final LongWritable outputValue = new LongWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
		
		if (line != null)
		{
			line = line.trim();
			
			String[] values = line.split("\\s");
			for (String word : values)
			{
				outputKey.set(word);
				context.write(outputKey, outputValue);
			}
		}
		
	}
	
}
