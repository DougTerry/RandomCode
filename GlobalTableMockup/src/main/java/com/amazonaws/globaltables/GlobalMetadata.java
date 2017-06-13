package com.amazonaws.globaltables;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.util.TableUtils;

public class GlobalMetadata {

	// Name of metadata table
	private static final String METADATA_TABLE_NAME = "GTMetadata";

	// Region where metadata is stored
	// Note: the metadata is not currently replicated in multiple regions
	private static final Regions METADATA_REGION = Regions.US_WEST_1;
	
	// Attributes stored in a metadata entry
	private static final String METADATA_KEY = "Tablename";
	private static final String METADATA_REGIONS = "Regions";
	private static final String METADATA_MASTER = "Master";
	
	// Note: Leases are not currently managed by this class.

	// Handle to a DynamoDB client for accessing metadata
	private AmazonDynamoDB ddb;

	public GlobalMetadata() {
		// Create DynamoDB client
		ddb = AmazonDynamoDBClientBuilder.standard()
				.withRegion(METADATA_REGION)
				.build();

        // Create metadata table if it does not exist
        CreateTableRequest createTableRequest = new CreateTableRequest()
        		.withTableName(METADATA_TABLE_NAME)
        		.withKeySchema(new KeySchemaElement()
            		.withAttributeName(METADATA_KEY)
            		.withKeyType(KeyType.HASH))
        		.withAttributeDefinitions(new AttributeDefinition()
            		.withAttributeName(METADATA_KEY)
            		.withAttributeType(ScalarAttributeType.S))
				.withProvisionedThroughput(new ProvisionedThroughput()
					.withReadCapacityUnits(1L)
					.withWriteCapacityUnits(1L));
        TableUtils.createTableIfNotExists(ddb, createTableRequest);
	}
	
	public boolean createTable(String tableName, Regions region) {
		// Check if global table already exists
		Map<String, AttributeValue> item = lookupMetadata(tableName);
		if (item != null) {
			return false;
		}
		
		// Create new global table with the given region as the master and only replica
		item = new HashMap<String, AttributeValue>();
        item.put(METADATA_KEY, new AttributeValue(tableName));
        item.put(METADATA_MASTER, new AttributeValue(region.getName()));
        item.put(METADATA_REGIONS, new AttributeValue().withSS(region.getName()));
        
        PutItemRequest putItemRequest = new PutItemRequest(METADATA_TABLE_NAME, item);
        ddb.putItem(putItemRequest);
        return true;
	}
	
	public boolean createTableAlt(String tableName, Regions region) {
		// Check if global table already exists
		Item item = lookupMetadataAlt(tableName);
		if (item != null) {
			return false;
		}
		
		// Create new global table with the given region as the master and only replica
		item = new Item()
				.withPrimaryKey(METADATA_KEY, tableName)
				.withString(METADATA_MASTER, region.getName())
				.withStringSet(METADATA_REGIONS, region.getName());
        
        DynamoDB ddbAlt = new DynamoDB(ddb);
        Table table = ddbAlt.getTable(METADATA_TABLE_NAME);
        table.putItem(item);
        return true;
	}
	
	public void addRegion(String tableName, Regions region) {
        HashMap<String,AttributeValue> key = new HashMap<String,AttributeValue>();
        key.put(METADATA_KEY, new AttributeValue(tableName));
        
        HashMap<String,AttributeValueUpdate> attributeUpdates = new HashMap<String,AttributeValueUpdate>();
        attributeUpdates.put(METADATA_REGIONS, new AttributeValueUpdate(new AttributeValue()
        		.withSS(region.getName()), AttributeAction.ADD));
        
        UpdateItemRequest updateItemRequest = new UpdateItemRequest()
        		.withTableName(METADATA_TABLE_NAME)
        		.withKey(key)
        		.withAttributeUpdates(attributeUpdates);
        ddb.updateItem(updateItemRequest);
	}
	
	public void removeRegion(String tableName, Regions region) {
        HashMap<String,AttributeValue> key = new HashMap<String,AttributeValue>();
        key.put(METADATA_KEY, new AttributeValue(tableName));
        
        HashMap<String,AttributeValueUpdate> attributeUpdates = new HashMap<String,AttributeValueUpdate>();
        attributeUpdates.put(METADATA_REGIONS, new AttributeValueUpdate(new AttributeValue()
        		.withSS(region.getName()), AttributeAction.DELETE));
        
        UpdateItemRequest updateItemRequest = new UpdateItemRequest()
        		.withTableName(METADATA_TABLE_NAME)
        		.withKey(key)
        		.withAttributeUpdates(attributeUpdates);
        ddb.updateItem(updateItemRequest);
	}
	
	public List<String> listRegions(String tableName) {
		Map<String, AttributeValue> item = lookupMetadata(tableName);
        return item.get(METADATA_REGIONS).getSS();
	}
	
	public Regions getMaster(String tableName) {
		Map<String, AttributeValue> item = lookupMetadata(tableName);
		return Regions.fromName(item.get(METADATA_MASTER).getS());
	}
	
	public boolean acquireMastership(String tableName, Regions region) {
        HashMap<String,AttributeValue> key = new HashMap<String,AttributeValue>();
        key.put(METADATA_KEY, new AttributeValue(tableName));
        
        HashMap<String,AttributeValueUpdate> attributeUpdates = new HashMap<String,AttributeValueUpdate>();
        attributeUpdates.put(METADATA_MASTER, new AttributeValueUpdate(new AttributeValue()
        		.withS(region.getName()), AttributeAction.PUT));
        
        UpdateItemRequest updateItemRequest = new UpdateItemRequest()
        		.withTableName(METADATA_TABLE_NAME)
        		.withKey(key)
        		.withAttributeUpdates(attributeUpdates);
        ddb.updateItem(updateItemRequest);
		return true;
	}
	
	public Map<String, AttributeValue> lookupMetadata(String tableName) {
		HashMap<String,AttributeValue> key = new HashMap<String,AttributeValue>();
        key.put(METADATA_KEY, new AttributeValue(tableName));
        
        GetItemRequest getItemRequest = new GetItemRequest()
        		.withTableName(METADATA_TABLE_NAME)
        		.withKey(key)
        		.withConsistentRead(true);
        GetItemResult getItemResult = ddb.getItem(getItemRequest);
        return getItemResult.getItem();
	}
	
	public Item lookupMetadataAlt(String tableName) {
		DynamoDB ddbAlt = new DynamoDB(ddb);
        Table table = ddbAlt.getTable(METADATA_TABLE_NAME);
        Item item = table.getItem(METADATA_KEY, tableName);
        return item;
	}
	
	public Item lookupMetadataAlt2(String tableName) {
		DynamoDB ddbAlt = new DynamoDB(ddb);
        Table table = ddbAlt.getTable(METADATA_TABLE_NAME);
        GetItemSpec getItemSpec = new GetItemSpec()
        		.withPrimaryKey(METADATA_KEY, tableName)
        		.withConsistentRead(true);
        Item item = table.getItem(getItemSpec);
        return item;
	}
	
}
