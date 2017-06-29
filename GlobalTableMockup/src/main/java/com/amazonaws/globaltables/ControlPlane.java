package com.amazonaws.globaltables;

import java.util.Set;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.util.TableUtils;

public class ControlPlane {

	public ControlPlane() {
		// does nothing
	}
	
	public void createAllReplicas(String tableName, String keyAttribute) {
		GlobalMetadata gmd = new GlobalMetadata();
		Set<Regions> regionSet = gmd.listRegions(tableName);
		for (Regions region : regionSet) {
			createRegionReplica(tableName, keyAttribute, region);						
		}
	}

	public boolean createRegionReplica(String tableName, String keyAttribute, Regions region) {
		AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.standard()
				.withRegion(region)
				.build();
		
		// create regional table if not already exists
		CreateTableRequest createTableRequest = new CreateTableRequest()
        		.withTableName(tableName)
        		.withKeySchema(new KeySchemaElement()
            		.withAttributeName(keyAttribute)
            		.withKeyType(KeyType.HASH))
        		.withAttributeDefinitions(new AttributeDefinition()
            		.withAttributeName(keyAttribute)
            		.withAttributeType(ScalarAttributeType.S))
				.withProvisionedThroughput(new ProvisionedThroughput()
					.withReadCapacityUnits(1L)
					.withWriteCapacityUnits(1L));
        boolean created = TableUtils.createTableIfNotExists(ddb, createTableRequest);

        // Wait for the table to move into active state
        if (created) {
        	try {
        		TableUtils.waitUntilActive(ddb, tableName);
        	} 
        	catch (InterruptedException e) {
        		System.out.println("Got interrupted while waiting for table to be created.");
        		System.out.println("Exception: " + e.getMessage());
        	}
        }
        return created;
	}
	
	public boolean deleteRegionReplica(String tableName, Regions region) {
		AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.standard()
				.withRegion(region)
				.build();
		DeleteTableRequest deleteTableRequest = new DeleteTableRequest()
        		.withTableName(tableName);
        boolean deleted = TableUtils.deleteTableIfExists(ddb, deleteTableRequest);
        return deleted;
	}
	
}
