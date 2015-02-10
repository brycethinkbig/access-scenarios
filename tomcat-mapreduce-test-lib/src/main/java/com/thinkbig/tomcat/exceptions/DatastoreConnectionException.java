package com.thinkbig.tomcat.exceptions;

public class DatastoreConnectionException extends RuntimeException
{

	private static final long serialVersionUID = 7763182879793110661L;

	public DatastoreConnectionException()
	{
		super();
	}

	public DatastoreConnectionException(String message, Throwable cause)
	{
		super(message, cause);
	}

	public DatastoreConnectionException(String message)
	{
		super(message);
	}

	public DatastoreConnectionException(Throwable cause)
	{
		super(cause);
	}
	
}
