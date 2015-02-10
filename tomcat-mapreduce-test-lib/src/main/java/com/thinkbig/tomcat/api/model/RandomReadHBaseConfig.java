package com.thinkbig.tomcat.api.model;

public class RandomReadHBaseConfig
{
	
	private String tablename;
	private String columnFamily;
	private String inputDirectory;
	private String outputDirectory;
	
	
	public RandomReadHBaseConfig(String tablename, String columnFamily, String inputDirectory, String outputDirectory)
	{
		this.tablename = tablename;
		this.columnFamily = columnFamily;
		this.inputDirectory = inputDirectory;
		this.outputDirectory = outputDirectory;
	}
	
	public String getTablename() { return tablename; }
	public String getColumnFamily() { return columnFamily; }
	public String getInputDirectory() { return inputDirectory; }
	public String getOutputDirectory() { return outputDirectory; }
	
}
