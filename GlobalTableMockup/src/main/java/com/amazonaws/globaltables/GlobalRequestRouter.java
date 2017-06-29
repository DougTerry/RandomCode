package com.amazonaws.globaltables;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;

public class GlobalRequestRouter {

	/**
	 * A global request router that sends eventually consistent reads/writes to the local region
	 * and strongly consistent reads/writes to the master region
	 */
	
	private String tableName;
	
	private String keyName;
	
	private Regions localRegion;
	private Regions masterRegion;
	
	private GlobalMetadata metadata;
	
	private Lease masterLease;
	
	// Handles to DynamoDB clients for the master and local regions
	private AmazonDynamoDB ddbMaster;
	private AmazonDynamoDB ddbLocal;
	
	// Handles to tables in master and local regions
	private Table masterReplica;
	private Table localReplica;
	
	
	/*
	 * Constructor for a global request router that "runs" in the given region
	 * and accesses the given table
	 */
	public GlobalRequestRouter(String table, Regions region, GlobalMetadata metadata) {
		tableName = table;
		localRegion = region;
		this.metadata = metadata;

		// Create DynamoDB client for local region
		ddbLocal = AmazonDynamoDBClientBuilder.standard()
				.withRegion(localRegion)
				.build();
        localReplica = new Table(ddbLocal, table);
		
		// For now, master region is unknown
		masterRegion = null;
		masterLease = null;
		ddbMaster = null;
        masterReplica = null;
        
        // Get primary key for table
        TableDescription desc = localReplica.describe();
        keyName = desc.getKeySchema().get(0).getAttributeName();
	}
	
	
	/*
	 * Operations that mimic some of those in the Table interface
	 */
	
	public Item getItem(String hashKeyName, Object hashKeyValue) {
        GetItemSpec getItemSpec = new GetItemSpec()
        		.withConsistentRead(false)
        		.withPrimaryKey(hashKeyName, hashKeyValue);
        return getItem(getItemSpec);
	}
	
	public Item getItem(GetItemSpec spec) {
		// Select replica based on desired consistency
		Table replica = localReplica;
		if (spec.isConsistentRead()) {
			refreshMasterEndpoint();
			replica = masterReplica;
		}
		
		// Do read
		Item item = replica.getItem(spec);
        return item;
	}
	
	public PutItemOutcome putItem(Item item) {
		ConsistentPutItemSpec putSpec = (ConsistentPutItemSpec) new ConsistentPutItemSpec()
				.withConsistentWrite(true)
				.withItem(item);
		return putItem(putSpec);
	}
	
	public PutItemOutcome putItem(ConsistentPutItemSpec spec) {
		// Select replica based on desired consistency
		Regions regionToWrite = localRegion;
		Table replica = localReplica;
		if (spec.isConsistentWrite()) {
			refreshMasterEndpoint();
			regionToWrite = masterRegion;
			replica = masterReplica;
		}
		
		// Add system attributes to item being written
		Item item = spec.getItem();
		VersionVector newVersion = bumpVersionVector(replica, item.getString(keyName), regionToWrite);
		SystemAttributes.setTimestamp(item, System.currentTimeMillis());
		SystemAttributes.setOrigin(item, regionToWrite.getName());
		SystemAttributes.setVersion(item, newVersion);  
		
		// Do write
		PutItemOutcome outcome = replica.putItem(spec);
        return outcome;
	}
	
	public UpdateItemOutcome updateItem(String hashKeyName, Object hashKeyValue, AttributeUpdate... attributeUpdates) {
        ConsistentUpdateItemSpec updateSpec = (ConsistentUpdateItemSpec) new ConsistentUpdateItemSpec()
				.withConsistentWrite(true)
        		.withPrimaryKey(hashKeyName, hashKeyValue)
				.withAttributeUpdate(attributeUpdates);
        return updateItem(updateSpec);
	}

	public UpdateItemOutcome updateItem(ConsistentUpdateItemSpec spec) {
		// Select replica based on desired consistency
		Regions regionToWrite = localRegion;
		Table replica = localReplica;
		if (spec.isConsistentWrite()) {
			refreshMasterEndpoint();
			regionToWrite = masterRegion;
			replica = masterReplica;
		}
		
		// Add updates for item's system attributes
		VersionVector newVersion = bumpVersionVector(replica, spec.getPrimaryKeyValue(), regionToWrite);
		spec.addAttributeUpdate(SystemAttributes.updateTimestamp());
		spec.addAttributeUpdate(SystemAttributes.updateOrigin(regionToWrite));
		spec.addAttributeUpdate(SystemAttributes.updateVersion(newVersion, regionToWrite));
		
		// Do write
		UpdateItemOutcome outcome = replica.updateItem(spec);
        return outcome;
	}

	
	/*
	 * Lower-level alternative operations (that are not needed)
	 */
	
	public GetItemResult getItem(GetItemRequest getItemRequest) {
		AmazonDynamoDB ddb = ddbLocal;
		if (getItemRequest.isConsistentRead()) {
			refreshMasterEndpoint();
			ddb = ddbMaster;
		}
		GetItemResult getItemResult = ddb.getItem(getItemRequest);
        return getItemResult;
	}
	
	public PutItemResult putItem(ConsistentPutItemRequest putItemRequest) {
		Regions regionToWrite = localRegion;
		AmazonDynamoDB ddb = ddbLocal;
		if (putItemRequest.isConsistentWrite()) {
			refreshMasterEndpoint();
			regionToWrite = masterRegion;
			ddb = ddbMaster;
		}
		Map<String, AttributeValue> item = putItemRequest.getItem();
		HashMap<String,AttributeValue> key = new HashMap<String,AttributeValue>();
        key.put("name", putItemRequest.getItem().get("name"));  // should not have to know about primary key
		AttributeValue versionVector = bumpVersionVector(putItemRequest.getTableName(), key, ddb, regionToWrite);
		SystemAttributes.setTimestamp(item, System.currentTimeMillis());
		SystemAttributes.setOrigin(item, regionToWrite.getName());
		SystemAttributes.setVersion(item, versionVector);  
		PutItemResult putItemResult = ddb.putItem(putItemRequest);
        return putItemResult;
	}
	
	public UpdateItemResult updateItem(ConsistentUpdateItemRequest updateItemRequest) {
		Regions regionToWrite = localRegion;
		AmazonDynamoDB ddb = ddbLocal;
		if (updateItemRequest.isConsistentWrite()) {
			refreshMasterEndpoint();
			regionToWrite = masterRegion;
			ddb = ddbMaster;
		}
		
		// Create item to temporarily hold system attribute values
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		SystemAttributes.setTimestamp(item, System.currentTimeMillis());
		SystemAttributes.setOrigin(item, regionToWrite.getName());
		AttributeValue versionVector = bumpVersionVector(updateItemRequest.getTableName(), updateItemRequest.getKey(), ddb, regionToWrite);
		SystemAttributes.setVersion(item, versionVector);  
		
		// Add updates for item's system attributes
		Map<String, AttributeValueUpdate> updates = updateItemRequest.getAttributeUpdates();
		for (String key : item.keySet()) {
			updates.put(key, new AttributeValueUpdate(item.get(key), AttributeAction.PUT));			
		}
		UpdateItemResult updateItemResult = ddb.updateItem(updateItemRequest);
        return updateItemResult;
	}
	
	/*
	 * Internal operations
	 */
	
	private void refreshMasterEndpoint() {
		if (masterLease == null) {
			masterLease = metadata.getLease(tableName);
		}
		if (masterRegion == null || masterLease.maybeExpired()) {
			Regions currentMaster = metadata.getMaster(tableName);
			if (currentMaster != masterRegion) {
				// master region has changed
				masterRegion = currentMaster;
				ddbMaster = AmazonDynamoDBClientBuilder.standard()
						.withRegion(masterRegion)
						.build();			
				masterReplica = new Table(ddbMaster, tableName);
			}
		}
	}
	
	private VersionVector bumpVersionVector(Table replica, String key, Regions region) {
		// Read current item
		GetItemSpec getSpec = new GetItemSpec()
				.withPrimaryKey(keyName, key)
				.withConsistentRead(true);
		Item storedItem = replica.getItem(getSpec);
		
		// Increment version vector
		VersionVector newVersion;
		if (storedItem == null) {
			newVersion = new VersionVector(region);
		} else {
			newVersion = SystemAttributes.getVersion(storedItem).bump(region);
		}
		return newVersion;
	}
	
	// deprecated
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
        	Map<String, AttributeValue> item = getItemResult.getItem();
        	versionVector = SystemAttributes.getVersion(item);
        } 
        if (versionVector == null) {
        	versionVector = new HashMap<String,AttributeValue>();
        }
        if (versionVector.containsKey(region.getName())) {
        	int count = Integer.parseInt(versionVector.get(region.getName()).getN());
        	versionVector.replace(region.getName(), new AttributeValue().withN(Integer.toString(count + 1)));       	
        } else {
        	versionVector.put(region.getName(), new AttributeValue().withN("1"));
        }
		
		return new AttributeValue().withM(versionVector);
	}

}
