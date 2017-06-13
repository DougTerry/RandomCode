package com.amazonaws.globaltables;

import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;

public class ConsistentUpdateItemRequest extends UpdateItemRequest{

	/**
	 * Adds support for eventually consistent writes
	 */
	
	private Boolean isConsistent = true;
	
	public ConsistentUpdateItemRequest() {
		// TODO Auto-generated constructor stub
	}
	
	public Boolean isConsistentWrite() {
		return isConsistent;
	}
	
	public ConsistentUpdateItemRequest withConsistentWrite(Boolean consistentWrite) {
		isConsistent = consistentWrite;
		return this;
	}

}
