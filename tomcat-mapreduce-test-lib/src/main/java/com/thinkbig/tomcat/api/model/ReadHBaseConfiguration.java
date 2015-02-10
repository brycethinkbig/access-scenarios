package com.thinkbig.tomcat.api.model;

public class ReadHBaseConfiguration
{
	
	private String tablename;
	private String columnFamily;
	private String outputDirectory;
	
	
	public ReadHBaseConfiguration(String tablename, String columnFamily, String outputDirectory)
	{
		this.tablename = tablename;
		this.columnFamily = columnFamily;
		this.outputDirectory = outputDirectory;
	}
	
	public String getTablename() { return tablename; }
	public String getColumnFamily() { return columnFamily; }
	public String getOutputDirectory() { return outputDirectory; }
	
}
