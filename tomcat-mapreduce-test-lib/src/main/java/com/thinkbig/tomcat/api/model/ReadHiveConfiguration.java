package com.thinkbig.tomcat.api.model;

public class ReadHiveConfiguration
{
	
	private String tablename;
	private String inputDirectory;
	private String outputDirectory;
	public ReadHiveConfiguration(String tablename, String inputDirectory, String outputDirectory)
	{
		this.tablename = tablename;
		this.inputDirectory = inputDirectory;
		this.outputDirectory = outputDirectory;
	}
	
	public String getTablename() { return tablename; }
	public String getInputDirectory() { return inputDirectory; }
	public String getOutputDirectory() { return outputDirectory; }
	
	
	
}
