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
import com.thinkbig.tomcat.api.impl.DefaultJobLauncherService;
import com.thinkbig.tomcat.api.impl.RandomReadHBaseJobLauncherService;
import com.thinkbig.tomcat.api.impl.ReadHBaseJobLauncherService;
import com.thinkbig.tomcat.api.impl.WriteHBaseJobLauncherService;
import com.thinkbig.tomcat.api.model.RandomReadHBaseConfig;
import com.thinkbig.tomcat.api.model.ReadHBaseConfiguration;
import com.thinkbig.tomcat.api.model.TomcatJobConfiguration;
import com.thinkbig.tomcat.api.model.WriteHBaseConfig;
import com.thinkbig.tomcat.util.NullSafe;

public class JobServlet extends HttpServlet
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private static final Logger logger = Logger.getLogger(JobServlet.class);
	
	private static final String PARAM_INPUT_DIR = "inputDirectory";
	private static final String PARAM_OUTPUT_DIR = "outputDirectory";
	private static final String PARAM_TABLE_NAME = "tableName";
	private static final String PARAM_COLUMN_FAMILY = "columnFamily";
	
	private JobLauncherService<TomcatJobConfiguration> jobService;
	
	@Inject
	public JobServlet(JobLauncherService<TomcatJobConfiguration> jobService)
	{
		this.jobService = jobService;
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
	{
		PrintWriter writer = null;
		try
		{
			String serviceName = getServiceName(req);
			writer = resp.getWriter();
			writer.println("about to launch job: " + serviceName);
			
			boolean success = launchService(req, writer);
			
			writer.println("job completed with success: " + success);
		}
		catch (InterruptedException e)
		{
			if (writer != null)
			{
				writer.println("exception: " + e);
				e.printStackTrace(writer);
			}
			throw new ServletException("error running job", e);
		}
	}
	
	protected String getServiceName(HttpServletRequest req)
	{
		String name = DefaultJobLauncherService.class.getSimpleName();
		String uri = req.getRequestURI();
		
		String[] pathElements = uri.split("/");
		
		if (!NullSafe.isEmpty(pathElements))
		{
			name = pathElements[pathElements.length - 1];
		}
		
		return name;
	}
	
	protected boolean launchService(HttpServletRequest req, PrintWriter out) throws IOException, InterruptedException
	{
		boolean result = false;
		
		String serviceName = getServiceName(req);
		
		if (ReadHBaseJobLauncherService.class.getSimpleName().equalsIgnoreCase(serviceName))
		{
			final String tablename = req.getParameter(PARAM_TABLE_NAME);
			final String columnFamily = req.getParameter(PARAM_COLUMN_FAMILY);
			final String outputDirectory = req.getParameter(PARAM_OUTPUT_DIR);
			
			out.println("tablename: " + tablename);
			out.println("columnFamily: " + columnFamily);
			out.println("outputDirectory: " + outputDirectory);
			
			ReadHBaseConfiguration config = new ReadHBaseConfiguration(tablename, columnFamily, outputDirectory);
			ReadHBaseJobLauncherService service = new ReadHBaseJobLauncherService();
			
			result = service.launchJob(config);
		}
		else if (RandomReadHBaseJobLauncherService.class.getSimpleName().equalsIgnoreCase(serviceName))
		{
			final String inputDirectory = req.getParameter(PARAM_INPUT_DIR);
			final String outputDirectory = req.getParameter(PARAM_OUTPUT_DIR);
			final String tablename = req.getParameter(PARAM_TABLE_NAME);
			final String columnFamily = req.getParameter(PARAM_COLUMN_FAMILY);
			
			out.println("inputDirectory: " + inputDirectory);
			out.println("outputDirectory: " + outputDirectory);
			out.println("tablename: " + tablename);
			out.println("columnFamily: " + columnFamily);
			
			RandomReadHBaseJobLauncherService service = new RandomReadHBaseJobLauncherService();
			RandomReadHBaseConfig config = new RandomReadHBaseConfig(tablename, columnFamily, inputDirectory, outputDirectory);
			
			result = service.launchJob(config);
		}
		else if (WriteHBaseJobLauncherService.class.getSimpleName().equalsIgnoreCase(serviceName))
		{
			final String inputDirectory = req.getParameter(PARAM_INPUT_DIR);
			final String tablename = req.getParameter(PARAM_TABLE_NAME);
			final String columnFamily = req.getParameter(PARAM_COLUMN_FAMILY);
			
			out.println("inputDirectory: " + inputDirectory);
			out.println("tablename: " + tablename);
			out.println("columnFamily: " + columnFamily);
			
			WriteHBaseJobLauncherService service = new WriteHBaseJobLauncherService();
			WriteHBaseConfig config = new WriteHBaseConfig(inputDirectory, tablename, columnFamily);
			
			result = service.launchJob(config);
		}
		
		// TODO: add other scenarios here...
		else
		{
			// default case:
			final String inputDirectory = req.getParameter(PARAM_INPUT_DIR);
			final String outputDirectory = req.getParameter(PARAM_OUTPUT_DIR);
			
			out.println("inputDirectory: " + inputDirectory);
			out.println("outputDirectory: " + outputDirectory);
			
			DefaultJobLauncherService service = new DefaultJobLauncherService();
			TomcatJobConfiguration config = new TomcatJobConfiguration(inputDirectory, outputDirectory);
			
			result = service.launchJob(config);
		}
		
		return result;
	}
	
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
	{
		// TODO Auto-generated method stub
	}
	
}
