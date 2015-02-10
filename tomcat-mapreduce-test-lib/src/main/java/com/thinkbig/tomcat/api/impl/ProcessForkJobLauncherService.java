package com.thinkbig.tomcat.api.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.log4j.Logger;

import com.thinkbig.tomcat.api.JobLauncherService;
import com.thinkbig.tomcat.api.model.TomcatJobConfiguration;

public class ProcessForkJobLauncherService implements JobLauncherService<TomcatJobConfiguration>
{
	private static final Logger logger = Logger.getLogger(ProcessForkJobLauncherService.class);
	// probably a configuration that will be pulled in from the properties files or something:
	private static final String tomcatLibDir = "/usr/local/tomcat/webapps/ROOT/WEB-INF/lib/";
	
	// possibly provided per job launcher in real-life:
	private static final String jarName = "tomcat-mapreduce-test-lib-0.0.1-SNAPSHOT.jar";

	@Override
	public boolean launchJob(TomcatJobConfiguration jobConfiguration) throws IOException, InterruptedException
	{
		Runtime runtime = Runtime.getRuntime();
		String jarPath = tomcatLibDir + jarName;
		String[] args = new String[] {"hadoop", "jar", jarPath, jobConfiguration.getInputDirectory(), jobConfiguration.getOutputDirectory() };
		Process process = runtime.exec(args);
		
		byte[] buffer = new byte[2048];
		InputStream output = process.getInputStream();
		
		int length = 0;
		while ((length = output.read(buffer)) > -1)
		{
			String message = new String(Arrays.copyOf(buffer, length));
			logger.warn(message);
		}
		
		int result = process.waitFor();
		return result == 0;
	}

}
