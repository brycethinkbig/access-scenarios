package com.thinkbig.tomcat.api.model;

public class ReadHiveConfiguration
{
	
	private String hiveUrl;
	private String hiveUser;
	private String hivePassword;
	private String inputDirectory;
	private String outputDirectory;
	
	public ReadHiveConfiguration(String hiveUrl, String hiveUser, String hivePassword, String inputDirectory, String outputDirectory)
	{
		this.hiveUrl = hiveUrl;
		this.hiveUser = hiveUser;
		this.hivePassword = hivePassword;
		this.inputDirectory = inputDirectory;
		this.outputDirectory = outputDirectory;
	}
	
	public String getHiveUrl() { return hiveUrl; }
	public String getHiveUser() { return hiveUser; }
	public String getHivePassword() { return hivePassword; }
	public String getInputDirectory() { return inputDirectory; }
	public String getOutputDirectory() { return outputDirectory; }
	
	
	
}
