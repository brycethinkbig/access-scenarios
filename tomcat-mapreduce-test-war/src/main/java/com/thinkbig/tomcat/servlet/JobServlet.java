package com.thinkbig.tomcat.servlet;

import java.io.IOException;
import java.io.PrintWriter;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.thinkbig.tomcat.api.JobLauncherService;
import com.thinkbig.tomcat.api.model.TomcatJobConfiguration;

public class JobServlet extends HttpServlet
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private static final Logger logger = Logger.getLogger(JobServlet.class);
	
	private static final String PARAM_INPUT_DIR = "inputDirectory";
	private static final String PARAM_OUTPUT_DIR = "outputDirectory";
	
	private JobLauncherService<TomcatJobConfiguration> jobService;
	
	@Inject
	public JobServlet(JobLauncherService<TomcatJobConfiguration> jobService)
	{
		this.jobService = jobService;
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
	{
		final String inputDirectory = req.getParameter(PARAM_INPUT_DIR);
		final String outputDirectory = req.getParameter(PARAM_OUTPUT_DIR);
		TomcatJobConfiguration jobConfiguration = new TomcatJobConfiguration(inputDirectory, outputDirectory);
		
		try
		{
			PrintWriter writer = resp.getWriter();
			writer.println("about to launch job");
			
			boolean success = jobService.launchJob(jobConfiguration);
			
			writer.println("job completed with success: " + success);
		}
		catch (InterruptedException e)
		{
			throw new ServletException("error running job", e);
		}
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
	{
		// TODO Auto-generated method stub
	}
	
}
