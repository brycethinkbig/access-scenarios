package com.thinkbig.tomcat.mapreduce;

import java.io.IOException;
import java.net.InetAddress;

import javax.security.sasl.SaslException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

public class LongRunningMapper extends WriteHBaseMapper
{
	public static final String COUNTER_TABLE_NAME = "counter_table";
	public static final String COLUMN_FAMILY_NAME = "data";
	public static final String COUNTER_QUALIFIER_NAME = "counter";
	
	public static final byte[] COUNTER_TABLE = Bytes.toBytes(COUNTER_TABLE_NAME);
	public static final byte[] COLUMN_FAMILY = Bytes.toBytes(COLUMN_FAMILY_NAME);
	public static final byte[] COUNTER_QUALIFIER = Bytes.toBytes(COUNTER_QUALIFIER_NAME);
	public static final byte[] COUNTER_KEY = Bytes.toBytes("myCounter");
	
	private long sleepMillis = 10L * 1000L; // 10 seconds
	private HTableInterface counterTable;
	private UserGroupInformation userGroupInformation;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		super.setup(context);
		counterTable = hbaseConnection.getTable(COUNTER_TABLE_NAME);
		UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
		String username = currentUser == null ? null : currentUser.getUserName();
		logger.warn("currentUser: " + currentUser + " (" + username + ")");
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		counterTable.close();
		super.cleanup(context);
	}
	
	protected void invalidateTokens(Configuration config)
	{
		try
		{
			UserGroupInformation user = UserGroupInformation.getCurrentUser();
			for (Token<? extends TokenIdentifier> token : user.getTokens())
			{
				token.cancel(config);
			}
		}
		catch (Exception e)
		{
			throw new RuntimeException("error clearning tokens", e);
		}
	}
	
	protected void authenticate(final Configuration config) throws IOException, InterruptedException
	{
		if (userGroupInformation != null)
		{
			userGroupInformation.reloginFromKeytab();
		}
		else
		{
			final String localhost = InetAddress.getLocalHost().getHostName();
			final String principal = "bdp/" + localhost + "@CDH.PREPROD.WUDIP.COM"; //"bdp@CDH.PREPROD.WUDIP.COM";
			final String keytab = "/var/run/wufiles/bdp/bdp.keytab";
			
			final boolean kerberos = "kerberos".equals(config.get("hbase.security.authentication"));
			logger.warn("kerberios enabled: " + kerberos);
			
			config.set("hbase.myclient.keytab", keytab);
			config.set("hbase.myclient.principal", principal);
			
			if (kerberos)
			{
				// this is essentially a kinit: (apparently)
				logger.warn("kerberos authentication method found in config");
				UserGroupInformation.setConfiguration(config);
				logger.warn("about to login user from keytab");
				userGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
				logger.warn("about to login user for hbase");
				User.login(config, "hbase.myclient.keytab", "hbase.myclient.principal", localhost);
			}
		}
		
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		
		try
		{
			super.map(key, value, context);
			counterTable.incrementColumnValue(COUNTER_KEY, COLUMN_FAMILY, COUNTER_QUALIFIER, 1L);
		}
		catch (SaslException e)
		{
			logger.error("authentication failure, trying to re-connect...", e);
			authenticate(context.getConfiguration());
			// not exactly recursive, so this call is fine, it's just a re-try of the method in the try block above:
			super.map(key, value, context);
			counterTable.incrementColumnValue(COUNTER_KEY, COLUMN_FAMILY, COUNTER_QUALIFIER, 1L);
			counterTable.incrementColumnValue(Bytes.toBytes("reauthCount"), COLUMN_FAMILY, COUNTER_QUALIFIER, 1L);
		}
		
		// sleep for the configured number of millis:
		Thread.sleep(sleepMillis);
	}
	
}
