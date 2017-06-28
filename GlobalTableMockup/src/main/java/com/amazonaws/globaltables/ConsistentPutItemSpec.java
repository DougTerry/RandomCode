package com.amazonaws.globaltables;

import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;

/**
 * Adds support for eventually consistent writes
 */

public class ConsistentPutItemSpec extends PutItemSpec {

	private Boolean isConsistent = true;

	public ConsistentPutItemSpec() {
		// does nothing
	}

	public Boolean isConsistentWrite() {
		return isConsistent;
	}

	public ConsistentPutItemSpec withConsistentWrite(Boolean consistentWrite) {
		isConsistent = consistentWrite;
		return this;
	}

}
