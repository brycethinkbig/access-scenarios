package com.thinkbig.tomcat.api.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.Logger;

public class SimpleAccessTest
{
	private static final Logger logger = Logger.getLogger(SimpleAccessTest.class);
	
	private Configuration config;
	private String filePath;
	private String hiveUrl;
	
	public SimpleAccessTest(final String filePath, final String hiveUrl)
	{
		this.filePath = filePath;
		this.hiveUrl = hiveUrl;
		
		this.config = new YarnConfiguration(new Configuration());
		
		config.addResource("core-site.xml");
		config.addResource("hdfs-site.xml");
		config.addResource("mapred-site.xml");
		config.addResource("yarn-site.xml");
		
		config = HBaseConfiguration.create(config);
	}
	
	public void testFileSystem() throws IOException
	{
		FileSystem fileSystem = FileSystem.get(config);
		Path path = new Path(filePath);
		
		FileStatus[] files = fileSystem.listStatus(path);
		for (FileStatus file : files)
		{
			Path filePath = file.getPath();
			logger.warn("file: " + filePath.getName());
			FSDataInputStream in = fileSystem.open(filePath);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			
			String line = null;
			while ( (line = reader.readLine()) != null)
			{
				logger.warn(line);
			}
			
			logger.warn("");
		}
	}
	
	public void testHiveConnection() throws Exception
	{
		Class.forName("org.apache.hive.jdbc.HiveDriver");
		
		Connection connection = DriverManager.getConnection(hiveUrl);
		
		Statement statement = connection.createStatement();
		ResultSet resultSet = statement.executeQuery("show tables");
		
		while (resultSet.next())
		{
		}
	}
	
	public void testHBaseConnection() throws Exception
	{
		HConnection connection = HConnectionManager.createConnection(config);
		HTableInterface table = connection.getTable(Bytes.toBytes("entity_data"));
		
		byte[] name = table.getTableName();
		
		table.close();
		connection.close();
	}
	
	public void testHBaseScan() throws Exception
	{
		Scan scan = new Scan(Bytes.toBytes(""), Bytes.toBytes("Z"));
		
		HConnection connection = HConnectionManager.createConnection(config);
		HTableInterface table = connection.getTable(Bytes.toBytes("entity_data"));
		
		ResultScanner scanner = table.getScanner(scan);
		if (scanner != null)
		{
			Result result = scanner.next();
			if (result != null && !result.isEmpty())
			{
				// 
			}
		}
		
		connection.close();
	}
	
	public void testHBaseAdmin() throws Exception
	{
		HBaseAdmin admin = new HBaseAdmin(config);
		
		if (admin.tableExists(Bytes.toBytes("entity_data")))
		{
			
		}
		
		admin.close();
	}
	
	public static void main(String[] args)
	{
		final String filePath = args[0];
		final String hiveUrl = args[1];
		
		SimpleAccessTest test = new SimpleAccessTest(filePath, hiveUrl);
		
		try
		{
			test.testFileSystem();
		}
		catch (Exception e)
		{
			logger.warn("error running filesystem test", e);
		}
		
		try
		{
			test.testHiveConnection();
		}
		catch (Exception e)
		{
			logger.warn("error running hive test", e);
		}
		
		try
		{
			test.testHBaseConnection();
		}
		catch (Exception e)
		{
			logger.warn("error running hbase connection", e);
		}
		
		try
		{
			test.testHBaseScan();;
		}
		catch (Exception e)
		{
			logger.warn("error running hbase scan", e);
		}
		
		try
		{
			test.testHBaseAdmin();;
		}
		catch (Exception e)
		{
			logger.warn("error running hbase admin", e);
		}
	}

}
