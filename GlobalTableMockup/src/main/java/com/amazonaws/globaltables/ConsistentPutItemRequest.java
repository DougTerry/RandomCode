package com.amazonaws.globaltables;

import com.amazonaws.services.dynamodbv2.model.PutItemRequest;

public class ConsistentPutItemRequest extends PutItemRequest {

	/**
	 * Adds support for eventually consistent writes
	 */
	
	// private PutItemRequest putItemRequest;
	
	private Boolean isConsistent = true;
	
	public ConsistentPutItemRequest() {
		// putItemRequest = request;
	}
	
	public Boolean isConsistentWrite() {
		return isConsistent;
	}
	
	public ConsistentPutItemRequest withConsistentWrite(Boolean consistentWrite) {
		isConsistent = consistentWrite;
		return this;
	}

}
