package com.thinkbig.tomcat.api.model;

import org.codehaus.jackson.annotate.JsonProperty;

public class TomcatJobConfiguration
{
	
	@JsonProperty
	private String inputDirectory;
	
	@JsonProperty
	private String outputDirectory;
	
//	@JsonProperty
//	private String hadoopParameters;
	
	public TomcatJobConfiguration(final String inputDirectory, final String outputDirectory)
	{
		this.inputDirectory = inputDirectory;
		this.outputDirectory = outputDirectory;
	}
	
	public String getInputDirectory()
	{
		return inputDirectory;
	}
	
	public String getOutputDirectory()
	{
		return this.outputDirectory;
	}
	
}
