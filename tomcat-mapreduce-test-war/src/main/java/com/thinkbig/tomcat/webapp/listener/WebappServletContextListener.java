package com.thinkbig.tomcat.webapp.listener;

import javax.servlet.FilterRegistration;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.ServletRegistration;
import javax.servlet.annotation.WebListener;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;


//import com.sun.jersey.spi.container.servlet.ServletContainer;
import com.thinkbig.tomcat.config.WebAppConfiguration;
import com.thinkbig.tomcat.filter.LoggingFilter;
import com.thinkbig.tomcat.servlet.JobServlet;

@WebListener
public class WebappServletContextListener implements ServletContextListener
{
	private static ApplicationContext appContext = null;
	private static Logger logger = Logger.getLogger(WebappServletContextListener.class);

	@Override
	public void contextInitialized(ServletContextEvent event)
	{
		ServletContext servletContext = event.getServletContext();
		logger.warn("starting up servlet context");
		System.out.println("STARTING THE CONTEXT");
		
		// initialize the application context:
		appContext = new AnnotationConfigApplicationContext(WebAppConfiguration.class);
		
		LoggingFilter filter = appContext.getBean(LoggingFilter.class);
		JobServlet jobServlet = appContext.getBean(JobServlet.class);
		
//		ServletContainer jerseyContainer = appContext.getBean(ServletContainer.class);
		
//		ServletRegistration jerseyRegistration = servletContext.addServlet("restServlet", jerseyContainer);
//		jerseyRegistration.setInitParameter("com.sun.jersey.config.property.packages", JobLauncherResource.class.getPackage().getName());
//		jerseyRegistration.addMapping("/rest");
		
		FilterRegistration filterRegistration = servletContext.addFilter("loggingFilter", filter);
		filterRegistration.addMappingForUrlPatterns(null, false, "/*", "*");
		
		ServletRegistration registration = servletContext.addServlet("JobServlet", jobServlet);
		registration.addMapping("/jobs", "/jobs*", "/jobs/", "/jobs/*");
		
	}
	
	@Override
	public void contextDestroyed(ServletContextEvent event)
	{
		// TODO Auto-generated method stub
		
	}

}
