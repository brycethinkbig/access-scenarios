package com.thinkbig.tomcat.api.model;

public class WriteHBaseConfig
{
	private String inputDirectory;
	private String tablename;
	private String columnFamily;
	
	
	public WriteHBaseConfig(String inputDirectory, String tablename, String columnFamily)
	{
		this.inputDirectory = inputDirectory;
		this.tablename = tablename;
		this.columnFamily = columnFamily;
	}
	
	public String getInputDirectory() { return inputDirectory; }
	public String getTablename() { return tablename; }
	public String getColumnFamily() { return columnFamily; }
	
}
