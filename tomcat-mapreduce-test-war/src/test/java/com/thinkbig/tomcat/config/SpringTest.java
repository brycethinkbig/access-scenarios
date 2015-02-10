package com.thinkbig.tomcat.config;
import javax.inject.Inject;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.thinkbig.tomcat.config.TestConfiguration;
import com.thinkbig.tomcat.filter.LoggingFilter;


@RunWith(JUnit4.class)
public class SpringTest
{
	
	@Inject
	private LoggingFilter filter = null;
	
	@Test
	public void testInitialization()
	{
		ApplicationContext context = new AnnotationConfigApplicationContext(TestConfiguration.class);
		context.getAutowireCapableBeanFactory().autowireBean(this);
		
		Assert.assertNotNull(filter);
		
	}

}
