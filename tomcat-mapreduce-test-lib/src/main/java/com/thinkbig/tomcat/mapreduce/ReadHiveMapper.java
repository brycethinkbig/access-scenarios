package com.thinkbig.tomcat.mapreduce;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReadHiveMapper extends Mapper<LongWritable, Text, Text, Text>
{
	private static final String PREFIX = ReadHiveMapper.class.getSimpleName();
	public static final String HIVE_URL = PREFIX + ".hiveUrl";
	public static final String HIVE_USER = PREFIX + ".hiveUser";
	public static final String HIVE_PASSWORD = PREFIX + ".hivePassword";
	
	private Connection connection = null;
	
	private final Text keyOut = new Text();
	private final Text valueOut = new Text();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		super.setup(context);
		
		final Configuration config = context.getConfiguration();
		
		String hiveUrl = config.get(HIVE_URL);
		String hiveUser = config.get(HIVE_USER);
		String hivePassword = config.get(HIVE_PASSWORD);
		
		try
		{
			connection = DriverManager.getConnection(hiveUrl, hiveUser, hivePassword);
		}
		catch (SQLException e)
		{
			throw new IOException("error connecting to Hive", e);
		}
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		super.cleanup(context);
		
		try
		{
			if (connection != null)
			{
				connection.close();
			}
		}
		catch (SQLException e)
		{
			throw new IOException("unable to disconnect from Hive", e);
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		
		String sql = value.toString();
		keyOut.set(sql);
		
		try
		{
			Statement statement = connection.createStatement();
			
			if (statement.execute(sql))
			{
				// if we're here, there is a result set to inspect...
				ResultSet resultSet = statement.getResultSet();
				ResultSetMetaData metadata = resultSet.getMetaData();
				int columnCount = metadata.getColumnCount();
				
				while (resultSet.next())
				{
					StringBuffer buffer = new StringBuffer();
					
					for (int i = 1; i <= columnCount; i++)
					{
						if (i > 1)
						{
							buffer.append(",");
						}
						
						Object column = resultSet.getObject(i);
						buffer.append(column == null ? "" : column.toString());
					}
					
					valueOut.set(buffer.toString());
					
					context.write(keyOut, valueOut);
				}
				
			}
			else
			{
				// if we are here, there's possibly a result count (like effected rows)
				int updateCount = statement.getUpdateCount();
				
				valueOut.set("" + updateCount);
				context.write(keyOut, valueOut);
			}
		}
		catch (SQLException e)
		{
			throw new IOException("error executing query: " + sql, e);
		}
		
	}
	
}
