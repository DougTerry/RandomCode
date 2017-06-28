package com.amazonaws.globaltables;

import java.util.Iterator;
import java.util.Map;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanFilter;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

public class SystemAttributes {

	// System attributes that are added by the GRR
	private static final String UPDATE_TIMESTAMP = "zgtTimestamp";   // time of last update to item
	private static final String UPDATE_ORIGIN = "zgtOrigin";   // region where item was updated
	private static final String UPDATE_VERSION = "zgtVersion";  // version vector

	public SystemAttributes() {
	}
	
	public static Long getTimestamp(Item item) {
	    return item.getLong(UPDATE_TIMESTAMP);
	}

	public static void setTimestamp(Item item, Long timestamp) {
	    item.withLong(UPDATE_TIMESTAMP, timestamp);
	}

	public static void setTimestamp(Item item) {
	    item.withLong(UPDATE_TIMESTAMP, System.currentTimeMillis());
	}

	public static String getOrigin(Item item) {
	    return item.getString(UPDATE_ORIGIN);
	}

	public static void setOrigin(Item item, String regionName) {
	    item.withString(UPDATE_ORIGIN, regionName);
	}

	public static VersionVector getVersion(Item item) {
	    VersionVector version = new VersionVector().fromMap(item.getMapOfNumbers(UPDATE_VERSION, Integer.class));
		return version;
	}

	public static void setVersion(Item item, VersionVector version) {
	    item.withMap(UPDATE_VERSION, version.toMap());
	}
	
    public static AttributeUpdate updateTimestamp() {
    	AttributeUpdate update = new AttributeUpdate(UPDATE_TIMESTAMP).put(System.currentTimeMillis());
    	return update;
    }
    
    public static AttributeUpdate updateOrigin(Regions region) {
    	AttributeUpdate update = new AttributeUpdate(UPDATE_ORIGIN).put(region.getName());
    	return update;
    }
    
    public static AttributeUpdate updateVersion(VersionVector version, Regions replica) {
    	AttributeUpdate update = new AttributeUpdate(UPDATE_VERSION).put(version.toMap());
    	return update;
    }
    
	public static ScanFilter getTimestampFilter(Long minTimestamp) {
		ScanFilter filter = new ScanFilter(UPDATE_TIMESTAMP).gt(minTimestamp);
		return filter;
	}

	public static ScanFilter getOriginFilter(Regions origin) {
		ScanFilter filter = new ScanFilter(UPDATE_ORIGIN).eq(origin.getName());
		return filter;
	}
	
	/*
	 * Following are getters/setters for items that are attribute maps
	 * These can go away once Items are used everywhere.
	 */
	
	public static Long getTimestamp(Map<String, AttributeValue> item) {
	    return Long.parseLong(item.get(UPDATE_TIMESTAMP).getN());
	}

	public static void setTimestamp(Map<String, AttributeValue> item, Long timestamp) {
		item.put(UPDATE_TIMESTAMP, new AttributeValue().withN(Long.toString(timestamp)));
	}

	public static String getOrigin(Map<String, AttributeValue> item) {
	    return item.get(UPDATE_ORIGIN).getS();
	}

	public static void setOrigin(Map<String, AttributeValue> item, String regionName) {
		item.put(UPDATE_ORIGIN, new AttributeValue(regionName));
	}

	public static Map<String,AttributeValue> getVersion(Map<String, AttributeValue> item) {
		Map<String,AttributeValue> version = item.get(UPDATE_VERSION).getM();
		return version;
	}

	public static void setVersion(Map<String, AttributeValue> item, AttributeValue version) {
		item.put(UPDATE_VERSION, version);  
	}
	
	/*
	 * Add systems attributes to each item of an existing table
	 */
	public static void addToTable(String tableName, Regions region) {
		DynamoDB ddb = new DynamoDB(region);
		Table table = ddb.getTable(tableName);
		VersionVector version = new VersionVector();
		version.bump(region);
		ScanSpec scanSpec = new ScanSpec()
				.withConsistentRead(true);
		ItemCollection<ScanOutcome> scanResults = table.scan(scanSpec);
		
		Iterator<Item> iterator = scanResults.iterator();
		while (iterator.hasNext()) {
	        Item item = iterator.next();
	        setTimestamp(item, System.currentTimeMillis());
	        setOrigin(item, region.getName());
	        setVersion(item, version);
	        table.putItem(item);
		}
	}

}
