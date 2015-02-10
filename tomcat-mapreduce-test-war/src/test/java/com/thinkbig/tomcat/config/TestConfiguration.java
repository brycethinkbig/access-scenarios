package com.thinkbig.tomcat.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;


//import com.sun.jersey.spi.container.servlet.ServletContainer;
import com.thinkbig.tomcat.api.JobLauncherService;
import com.thinkbig.tomcat.api.impl.DefaultJobLauncherService;
import com.thinkbig.tomcat.api.impl.ProcessForkJobLauncherService;
import com.thinkbig.tomcat.filter.LoggingFilter;
import com.thinkbig.tomcat.servlet.JobServlet;

// Annotation based configuration 'cause that rocks HELLA better than stupid ol' XML lame configuration
// still not even half as good as Guice configuration, but less shitty than spring-xml configuration
@Configuration
@Component
public class TestConfiguration
{
	
	// Guice does a WAY better job of allowing builder-style, simple, declarative configurations and bindings,
	// spring still requires programatic construction of objects (lame, lame, LAME)
	@Bean
	public LoggingFilter loggingFilter()
	{
		return new LoggingFilter();
	}
	
}
