package com.amazonaws.globaltables;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
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
	private static final Regions METADATA_MASTER_REGION = Regions.US_WEST_1;
	private static final Regions METADATA_SECONDARY_REGION = Regions.US_EAST_2;
	
	// Attributes stored in a metadata entry
	private static final String METADATA_KEY = "Tablename";
	private static final String METADATA_REGIONS = "Regions";
	private static final String METADATA_MASTER = "Master";
	
	// Note: Leases are not currently managed by this class.

	// Metadata is itself stored as a global table and hence is accessed using a GRR
	private GlobalRequestRouter mdTable;

	public GlobalMetadata() {
        // Create metadata table if it does not exist
        createMetadataTableStore(METADATA_MASTER_REGION);
		
		// Create DynamoDB client
        mdTable = new GlobalRequestRouter(METADATA_TABLE_NAME, METADATA_KEY, METADATA_MASTER_REGION);
	}
	
	public boolean createTable(String tableName, Regions region) {
		// Check if global table already exists
		Item item = lookupMetadata(tableName);
		if (item != null) {
			return false;
		}
		
		// Create new global table with the given region as the master and only replica
		item = new Item()
				.withPrimaryKey(METADATA_KEY, tableName)
				.withString(METADATA_MASTER, region.getName())
				.withStringSet(METADATA_REGIONS, region.getName());
        putItem(item);
		return true;
	}
	
	public void addRegion(String tableName, Regions region) {
        AttributeUpdate update = new AttributeUpdate(METADATA_REGIONS).addElements(region.getName());
        updateItem(tableName, update);
	}
	
	public void removeRegion(String tableName, Regions region) {
        AttributeUpdate update = new AttributeUpdate(METADATA_REGIONS).removeElements(region.getName());
        updateItem(tableName, update);
	}
	
	public Set<Regions> listRegions(String tableName) {
		Item item = lookupMetadata(tableName);
		if (item == null) {
			return null;
		}
		Set<Regions> regionSet = new HashSet<Regions>();
		for (String regionName : item.getStringSet(METADATA_REGIONS)) {
			regionSet.add(Regions.fromName(regionName));
		}
        return regionSet;
	}
	
	public Regions getMaster(String tableName) {
		Item item = lookupMetadata(tableName);
		return Regions.fromName(item.getString(METADATA_MASTER));
	}
	
	public boolean acquireMastership(String tableName, Regions region) {
        AttributeUpdate update = new AttributeUpdate(METADATA_MASTER).put(region.getName());
        updateItem(tableName, update);
		return true;
	}
	
	
	/*
	 * Private methods
	 */
	
	private Item lookupMetadata(String tableName) {
        GetItemSpec getItemSpec = new GetItemSpec()
        		.withPrimaryKey(METADATA_KEY, tableName)
        		.withConsistentRead(true);
        Item item = mdTable.getItem(getItemSpec);
        return item;
	}
	
	private PutItemOutcome putItem(Item item) {
		ConsistentPutItemSpec putSpec = (ConsistentPutItemSpec) new ConsistentPutItemSpec()
				.withConsistentWrite(true)
				.withItem(item);
        return mdTable.putItem(putSpec);
	}
	
	private UpdateItemOutcome updateItem(String tableName, AttributeUpdate... attributeUpdates) {
        ConsistentUpdateItemSpec updateSpec = (ConsistentUpdateItemSpec) new ConsistentUpdateItemSpec()
				.withConsistentWrite(true)
				.withPrimaryKey(METADATA_KEY, tableName)
				.withAttributeUpdate(attributeUpdates);
        return mdTable.updateItem(updateSpec);
	}
	
	private void createMetadataTableStore(Regions region) {
        // Create metadata table if it does not exist
		AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.standard()
				.withRegion(region)
				.build();
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
	

	/*
	 * Methods for creating and replicating metadata as a global table
	 */
	
	public void bootstrapMetadata() {
		if (lookupMetadata(METADATA_TABLE_NAME) == null) {
			createTable(METADATA_TABLE_NAME, METADATA_MASTER_REGION);
			addRegion(METADATA_TABLE_NAME, METADATA_SECONDARY_REGION);
			createMetadataTableStore(METADATA_SECONDARY_REGION);
			SystemAttributes.addToTable(METADATA_TABLE_NAME, METADATA_MASTER_REGION);		
		};
	}

	public int replicateMetadata() {
		ReplicationEngine re = new ReplicationEngine();
		int num = re.syncReplicas(GlobalMetadata.METADATA_TABLE_NAME);
		return num;
	}
	
	public Regions getMetadataMaster() {
		return METADATA_MASTER_REGION;
	}
	
	public Regions getMetadataSecondary() {
		return METADATA_SECONDARY_REGION;
	}
	
}
