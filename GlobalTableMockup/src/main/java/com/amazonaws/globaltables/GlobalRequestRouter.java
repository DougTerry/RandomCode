package com.amazonaws.globaltables;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;

public class GlobalRequestRouter {

	/**
	 * A global request router that sends eventually consistent reads/writes to the local region
	 * and strongly consistent reads/writes to the master region
	 */
	
	// System attributes that are added by the GRR
	private static final String UPDATE_TIMESTAMP = "zgtTimestamp";   // time of last update to item
	private static final String UPDATE_ORIGIN = "zgtOrigin";   // region where item was updated
	private static final String UPDATE_VERSION = "zgtVersion";  // version vector
	
	private String tableName;
	
	private Regions localRegion;
	private Regions masterRegion;
	
	private GlobalMetadata metadata;
	
	// Handles to DynamoDB clients for the master and local regions
	private AmazonDynamoDB ddbMaster;
	private AmazonDynamoDB ddbLocal;

	public GlobalRequestRouter(String table, Regions region) {
		tableName = table;
		localRegion = region;
		metadata = new GlobalMetadata();
		masterRegion = metadata.getMaster(table);
		
		// Create DynamoDB client for local region
		ddbLocal = AmazonDynamoDBClientBuilder.standard()
				.withRegion(localRegion)
				.build();
		
		// Create DynamoDB client for master region
		ddbMaster = AmazonDynamoDBClientBuilder.standard()
				.withRegion(masterRegion)
				.build();
	}
	
	public GetItemResult getItem(GetItemRequest getItemRequest) {
		AmazonDynamoDB ddb = ddbLocal;
		if (getItemRequest.isConsistentRead()) {
			refreshMasterEndpoint(tableName);
			ddb = ddbMaster;
		}
		GetItemResult getItemResult = ddb.getItem(getItemRequest);
        return getItemResult;
	}
	
	public PutItemResult putItem(ConsistentPutItemRequest putItemRequest) {
		Regions regionToWrite = localRegion;
		AmazonDynamoDB ddb = ddbLocal;
		if (putItemRequest.isConsistentWrite()) {
			refreshMasterEndpoint(tableName);
			regionToWrite = masterRegion;
			ddb = ddbMaster;
		}
		Map<String, AttributeValue> item = putItemRequest.getItem();
		HashMap<String,AttributeValue> key = new HashMap<String,AttributeValue>();
        key.put("name", putItemRequest.getItem().get("name"));  // should not have to know about primary key
		AttributeValue versionVector = bumpVersionVector(putItemRequest.getTableName(), key, ddb, regionToWrite);
		item.put(UPDATE_TIMESTAMP, new AttributeValue().withN(Long.toString(System.currentTimeMillis())));
		item.put(UPDATE_ORIGIN, new AttributeValue(regionToWrite.getName()));
		item.put(UPDATE_VERSION, versionVector);  
		PutItemResult putItemResult = ddb.putItem(putItemRequest);
        return putItemResult;
	}
	
	public UpdateItemResult updateItem(ConsistentUpdateItemRequest updateItemRequest) {
		Regions regionToWrite = localRegion;
		AmazonDynamoDB ddb = ddbLocal;
		if (updateItemRequest.isConsistentWrite()) {
			refreshMasterEndpoint(tableName);
			regionToWrite = masterRegion;
			ddb = ddbMaster;
		}
		Map<String, AttributeValueUpdate> updates = updateItemRequest.getAttributeUpdates();
		AttributeValue versionVector = bumpVersionVector(updateItemRequest.getTableName(), updateItemRequest.getKey(), ddb, regionToWrite);
		updates.put(UPDATE_TIMESTAMP, new AttributeValueUpdate(new AttributeValue().withN(Long.toString(System.currentTimeMillis())), AttributeAction.PUT));
		updates.put(UPDATE_ORIGIN, new AttributeValueUpdate(new AttributeValue(regionToWrite.getName()), AttributeAction.PUT));
		updates.put(UPDATE_VERSION, new AttributeValueUpdate(versionVector, AttributeAction.PUT));  
		UpdateItemResult updateItemResult = ddb.updateItem(updateItemRequest);
        return updateItemResult;
	}
	
	private void refreshMasterEndpoint(String table) {
		Regions currentMaster = metadata.getMaster(table);
		if (currentMaster != masterRegion) {
			// master region has changed
			masterRegion = currentMaster;
			ddbMaster = AmazonDynamoDBClientBuilder.standard()
					.withRegion(masterRegion)
					.build();			
		}
	}
	
	private AttributeValue bumpVersionVector(String tableName, Map<String,AttributeValue> primaryKey, AmazonDynamoDB ddb, Regions region) {
		Map<String,AttributeValue> versionVector = null;
		
		// Read item to get current version vector
        GetItemRequest getItemRequest = new GetItemRequest()
        		.withTableName(tableName)
        		.withKey(primaryKey)
        		.withConsistentRead(true);
        GetItemResult getItemResult = ddb.getItem(getItemRequest);

		// Now bump the local region's entry
        if (getItemResult.getItem() != null) {
        	versionVector = getItemResult.getItem().get(UPDATE_VERSION).getM();
        } 
        if (versionVector == null) {
        	versionVector = new HashMap<String,AttributeValue>();
        }
        if (versionVector.containsKey(region.getName())) {
        	int count = Integer.parseInt(versionVector.get(region.getName()).getN());
        	count++;
        	versionVector.replace(region.getName(), new AttributeValue().withN(Integer.toString(count + 1)));       	
        } else {
        	versionVector.put(region.getName(), new AttributeValue().withN("1"));
        }
		
		return new AttributeValue().withM(versionVector);
	}

}
