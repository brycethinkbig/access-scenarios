package com.thinkbig.tomcat.api.model;

public class LocalReadHBaseConfig
{
	private String tablename;
	private String columnFamily;
	
	
	public LocalReadHBaseConfig(String tablename, String columnFamily)
	{
		this.tablename = tablename;
		this.columnFamily = columnFamily;
	}
	
	public String getTablename() { return tablename; }
	public String getColumnFamily() { return columnFamily; }
	
}
