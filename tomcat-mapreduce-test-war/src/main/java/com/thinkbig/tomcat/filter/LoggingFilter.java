package com.thinkbig.tomcat.filter;

import java.io.IOException;

import javax.inject.Inject;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;

public class LoggingFilter implements Filter
{
	private static final Logger logger = Logger.getLogger(LoggingFilter.class);
	
	@Inject
	public LoggingFilter()
	{
		
	}

	@Override
	public void init(FilterConfig context) throws ServletException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void destroy()
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain) throws IOException, ServletException
	{
		HttpServletRequest request = (HttpServletRequest) req;
		logger.warn("request.servletPath: " + request.getServletPath());
		System.out.println("REQUEST.SERVLET_PATH: " + request.getServletPath());
		chain.doFilter(req, resp);
	}

}
