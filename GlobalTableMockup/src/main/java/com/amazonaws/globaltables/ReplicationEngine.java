package com.amazonaws.globaltables;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.ScanFilter;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

public class ReplicationEngine {
	
	// table name -> target region -> source region -> timestamp
	private Map<String, Map<String, Map<String, Long>>> highTimestamps;
	
	// System attributes that are added by the GRR
	private static final String UPDATE_TIMESTAMP = "zgtTimestamp";   // time of last update to item
	private static final String UPDATE_ORIGIN = "zgtOrigin";   // region where item was updated
	private static final String UPDATE_VERSION = "zgtVersion";  // version vector

	public ReplicationEngine() {
		highTimestamps = new HashMap<String, Map<String, Map<String, Long>>>();
	}
	
	public int pullItems(String tableName, Regions target, Regions source) {
		Map<String, Map<String, Long>> tableTimestamps;
		if (!highTimestamps.containsKey(tableName)) {
			tableTimestamps = getTableTimestamps(tableName);
			highTimestamps.put(tableName, tableTimestamps);
		} else {
			tableTimestamps = highTimestamps.get(tableName);
		}
		
		// Scan source for recently updated items
		/*
		AmazonDynamoDB ddbSource = AmazonDynamoDBClientBuilder.standard()
				.withRegion(source)
				.build();
		Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
		Long minTime = tableTimestamps.get(target.getName()).get(source.getName());
		expressionAttributeValues.put(":mintime", new AttributeValue().withN(Long.toString(minTime)));		
		ScanRequest scanRequest = new ScanRequest()
			    .withTableName(tableName)
			    .withExpressionAttributeValues(expressionAttributeValues)
			    .withFilterExpression(UPDATE_TIMESTAMP + " > :mintime");
		ScanResult scanResult = ddbSource.scan(scanRequest);		
		 */
		
		// Use better scan technique
		DynamoDB ddbSource = new DynamoDB(source);
		Table sourceTable = ddbSource.getTable(tableName);
		Long lastSyncTime = tableTimestamps.get(target.getName()).get(source.getName());
		ScanFilter recentTimestamp = new ScanFilter(UPDATE_TIMESTAMP).gt(lastSyncTime);
		ScanFilter updatedBySource = new ScanFilter(UPDATE_ORIGIN).eq(source.getName());
		ScanSpec scanSpec = new ScanSpec()
				.withConsistentRead(true)
				.withScanFilters(updatedBySource, recentTimestamp);
		ItemCollection<ScanOutcome> scanResults = sourceTable.scan(scanSpec);
		
		// Write items to target
		/*
		AmazonDynamoDB ddbTarget = AmazonDynamoDBClientBuilder.standard()
				.withRegion(target)
				.build();
		Long maxTimestamp = minTime;
		for (Map<String, AttributeValue> item : scanResult.getItems()) {
	        PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
	        ddbTarget.putItem(putItemRequest);
		    Long itemTimestamp = Long.parseLong(item.get(UPDATE_TIMESTAMP).getN());
		    if (itemTimestamp > maxTimestamp) {
		    	maxTimestamp = itemTimestamp;
		    }
		}
		*/
		Table targetTable = new DynamoDB(target).getTable(tableName);
		String keyAttribute = targetTable.describe().getKeySchema().get(0).getAttributeName();
		ConflictResolver resolver = new ConflictResolver();
		Long maxTimestamp = lastSyncTime;
		int numReplicated = 0;
		int numConflicts = 0;

		Iterator<Item> iterator = scanResults.iterator();
		while (iterator.hasNext()) {
	        Item sourceItem = iterator.next();
	        boolean doUpdate = true;
	        
	        // Check for conflict with item stored in target table
	        Item targetItem = targetTable.getItem(keyAttribute, sourceItem.getString(keyAttribute));
	        if (resolver.isConflict(sourceItem, targetItem)) {
	        	GlobalMetadata gmd = new GlobalMetadata();
	        	System.out.println("Conflict detected to item " + sourceItem.getString(keyAttribute));
	        	System.out.println("   when replicating from  " + source + " to " + target);
	        	numConflicts++;
	        	if (!resolver.isWinner(sourceItem, targetItem, gmd.getMaster(tableName))) {
	        		doUpdate = false;
	        		System.out.println("Version at " + target + " was retained as the winner.");
	        	} else {
	        		System.out.println("Version from " + source + " was written as the winner.");	        		
	        	}
	        }
	        
	        // Perform update
	        if (doUpdate) {
	        	targetTable.putItem(sourceItem);
	        }
        	
	        // Update variables
	        Long itemTimestamp = SystemAttributes.getTimestamp(sourceItem);
        	if (itemTimestamp > maxTimestamp) {
        		maxTimestamp = itemTimestamp;
        	}
        	numReplicated++;
		}
		
		// Update high timestamp
    	tableTimestamps.get(target.getName()).replace(source.getName(), maxTimestamp);
    	
    	return numReplicated;
	}
	
	private Map<String, Map<String, Long>> getTableTimestamps(String tableName) {
		Map<String, Map<String, Long>> timestamps = new HashMap<String, Map<String, Long>>();
		
		// Get regions for table
		GlobalMetadata gmd = new GlobalMetadata();
		List<String> replicaSet = gmd.listRegions(tableName);
		
		// Initialize timestamps to all zeros
		for (String target : replicaSet) {
			HashMap<String, Long> timesForTarget = new HashMap<String, Long>();
			for (String source : replicaSet) {
				timesForTarget.put(source, 0L);
			}
			timestamps.put(target, timesForTarget);
		}
		
		// Scan replicas to update latest timestamps from each region
		for (String target : replicaSet) {
			AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.standard()
					.withRegion(Regions.fromName(target))
					.build();
			ScanRequest scanRequest = new ScanRequest()
				    .withTableName(tableName);
			ScanResult scanResult = ddb.scan(scanRequest);
			for (Map<String, AttributeValue> item : scanResult.getItems()){
			    Long itemTimestamp = Long.parseLong(item.get(UPDATE_TIMESTAMP).getN());
			    String itemOrigin = item.get(UPDATE_ORIGIN).getS();
			    if (itemTimestamp > timestamps.get(target).get(itemOrigin)) {
			    	timestamps.get(target).replace(itemOrigin, itemTimestamp);
			    }
			}
		}
		
		return timestamps;
	}

}
