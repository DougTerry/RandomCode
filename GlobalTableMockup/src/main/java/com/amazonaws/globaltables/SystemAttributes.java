package com.amazonaws.globaltables;

import com.amazonaws.services.dynamodbv2.document.Item;

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

	public static String getOrigin(Item item) {
	    return item.getString(UPDATE_ORIGIN);
	}

	public static void setOrigin(Item item, String regionName) {
	    item.withString(UPDATE_ORIGIN, regionName);
	}

	public static VersionVector getVersion(Item item) {
	    // VersionVector version = new VersionVector().fromMap(item.getMap(UPDATE_VERSION));
	    VersionVector version = new VersionVector().fromMap(item.getMapOfNumbers(UPDATE_VERSION, Integer.class));
		return version;
	}

	public static void setVersion(Item item, VersionVector version) {
	    item.withMap(UPDATE_VERSION, version.toMap());
	}

}
