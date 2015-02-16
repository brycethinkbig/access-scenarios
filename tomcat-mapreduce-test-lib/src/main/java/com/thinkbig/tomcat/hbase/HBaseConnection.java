package com.thinkbig.tomcat.hbase;

import java.io.Closeable;
import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.thinkbig.tomcat.exceptions.DatastoreConnectionException;

public class HBaseConnection implements Closeable
{
	private static final Logger logger = Logger.getLogger(HBaseConnection.class);
	private HConnection connection;
	
	@Inject
	public HBaseConnection(HConnection connection)
	{
		this.connection = connection;
	}
	
	public HTableInterface getTable(String tablename) throws DatastoreConnectionException
	{
		HTableInterface table = null;
		
		try
		{
			table = connection.getTable(tablename);
		}
		catch (IOException e)
		{
			throw new DatastoreConnectionException("error getting table: " + tablename, e);
		}
		
		return table;
	}
	
	public boolean tableExists(String tablename) throws DatastoreConnectionException
	{
		boolean exists = false;
		HBaseAdmin admin = null;
		
		try
		{
			admin = getAdmin();
			exists = admin.tableExists(tablename);
		}
		catch (Exception e)
		{
			final String message = "error connecting to admin server";
			logger.log(Level.WARN, message, e);
			throw new DatastoreConnectionException(message, e);
		}
		finally
		{
			close(admin);
		}
		
		return exists;
	}
	
	public void createTable(String tablename, String columnFamily) throws DatastoreConnectionException
	{
		HBaseAdmin admin = null;
		try
		{
			admin = getAdmin();
			
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tablename));
			HColumnDescriptor family = new HColumnDescriptor(columnFamily);
			table.addFamily(family);
			admin.createTable(table);
		}
		catch (Exception e)
		{
			final String message = "error creating table: " + tablename;
			logger.log(Level.WARN, message, e);
			throw new DatastoreConnectionException(message, e);
		}
		finally
		{
			close(admin);
		}
	}
	
	public void deleteTable(String tablename) throws DatastoreConnectionException
	{
		HBaseAdmin admin = null;
		try
		{
			final byte[] name = Bytes.toBytes(tablename);
			admin = getAdmin();
			admin.disableTable(name);
			admin.deleteTable(name);
		}
		catch (Exception e)
		{
			final String message = "error creating table: " + tablename;
			logger.log(Level.WARN, message, e);
			throw new DatastoreConnectionException(message, e);
		}
		finally
		{
			close(admin);
		}
	}
	
	public void close() throws IOException
	{
		connection.close();
	}
	
	protected HBaseAdmin getAdmin() throws IOException
	{
		return new HBaseAdmin(connection.getConfiguration());
	}
	
	protected static void close(Closeable closeable)
	{
		try
		{
			if (closeable != null)
			{
				closeable.close();
			}
		}
		catch (Exception e)
		{
			logger.log(Level.WARN, "error closing: " + closeable, e);
		}
	}
	
}
