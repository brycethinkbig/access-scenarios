package com.thinkbig.tomcat.util;

import java.util.Collection;
import java.util.Map;

public class NullSafe
{
	
	public static final boolean isEmpty(final String value)
	{
		return value == null || value.isEmpty() || value.trim().isEmpty();
	}
	
	public static final boolean isEmpty(final Object[] values)
	{
		return values == null || values.length == 0;
	}
	
	public static final boolean isEmpty(final Collection<?> collection)
	{
		return collection == null || collection.isEmpty();
	}
	
	public static final boolean isEmpty(Map<?, ?> map)
	{
		return map == null || map.isEmpty();
	}

}
