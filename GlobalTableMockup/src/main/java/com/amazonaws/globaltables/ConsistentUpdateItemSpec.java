package com.amazonaws.globaltables;

import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;

/**
 * Adds support for eventually consistent writes
 */

public class ConsistentUpdateItemSpec extends UpdateItemSpec{

	private Boolean isConsistent = true;
	
	private String keyValue = null;

	public ConsistentUpdateItemSpec() {
		// does nothing
	}

	public Boolean isConsistentWrite() {
		return isConsistent;
	}

	public ConsistentUpdateItemSpec withConsistentWrite(Boolean consistentWrite) {
		isConsistent = consistentWrite;
		return this;
	}
	
	/*
	 * NOTE: UpdateItemSpec allows clients to set the primary key but provides no way to get it.
	 * So, this adds a getPrimaryKey method.
	 */
	
	public ConsistentUpdateItemSpec withPrimaryKey(String hashKeyName, String hashKeyValue) {
		keyValue = hashKeyValue;
		super.withPrimaryKey(hashKeyName, hashKeyValue);
		return this;
	}
	
	public String getPrimaryKeyValue() {
		return keyValue;
	}

}
