package com.thinkbig.tomcat.api;

import java.io.IOException;

public interface JobLauncherService<T>
{
	boolean launchJob(T jobConfiguration, boolean forTest) throws IOException, InterruptedException;
}
