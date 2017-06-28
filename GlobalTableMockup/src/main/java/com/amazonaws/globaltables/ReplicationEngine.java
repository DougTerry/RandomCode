package com.amazonaws.globaltables;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
	
	private GlobalMetadata gmd;
	
	public ReplicationEngine() {
		highTimestamps = new HashMap<String, Map<String, Map<String, Long>>>();
		gmd = new GlobalMetadata();
	}
	
	public void generateTimestamps(String tableName) {
		if (!highTimestamps.containsKey(tableName)) {
			highTimestamps.put(tableName, getTableTimestamps(tableName));
		}		
	}
		
	/*
	 * Replicate recently updated items from the source region to the target region.
	 */
	public int pullItems(String tableName, Regions target, Regions source) {
		Map<String, Map<String, Long>> tableTimestamps;
		if (!highTimestamps.containsKey(tableName)) {
			tableTimestamps = getTableTimestamps(tableName);
			highTimestamps.put(tableName, tableTimestamps);
		} else {
			tableTimestamps = highTimestamps.get(tableName);
		}
		
		// Scan source for recently updated items
		DynamoDB ddbSource = new DynamoDB(source);
		Table sourceTable = ddbSource.getTable(tableName);
		Long lastSyncTime = tableTimestamps.get(target.getName()).get(source.getName());
		ScanFilter recentTimestamp = SystemAttributes.getTimestampFilter(lastSyncTime);
		ScanFilter updatedBySource = SystemAttributes.getOriginFilter(source);
		ScanSpec scanSpec = new ScanSpec()
				.withConsistentRead(true)
				.withScanFilters(updatedBySource, recentTimestamp);
		ItemCollection<ScanOutcome> scanResults = sourceTable.scan(scanSpec);
		
		// Write items to target
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
        	//System.out.println("Source item " + sourceItem);
        	//System.out.println("Target item " + targetItem);
	        if (resolver.isConflict(sourceItem, targetItem)) {
	        	System.out.println("     Conflict detected to item " + sourceItem.getString(keyAttribute));
	        	System.out.println("         when replicating from  " + source + " to " + target);
	        	numConflicts++;
	        	if (!resolver.isWinner(sourceItem, targetItem, gmd.getMaster(tableName))) {
	        		doUpdate = false;
	        		System.out.println("     Version at " + target + " was retained as the winner.");
	        	} else {
	        		System.out.println("     Version from " + source + " was written as the winner.");	        		
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
	
	/*
	 * Replicate recently updated items between all pairs of replicas for the given table.
	 */
	public int syncReplicas(String tableName) {
		Set<Regions> replicaSet = gmd.listRegions(tableName);
		int numReplicated = 0;
		
		if (replicaSet == null) {
			return 0;
		}
		
		for (Regions target : replicaSet) {
			for (Regions source : replicaSet) {
				if (!target.equals(source)) {
					int num = this.pullItems(tableName, target, source);
					numReplicated += num;
				}
			}
		}
		
    	return numReplicated;
	}
	
	private Map<String, Map<String, Long>> getTableTimestamps(String tableName) {
		Map<String, Map<String, Long>> timestamps = new HashMap<String, Map<String, Long>>();
		
		// Get regions for table
		GlobalMetadata gmd = new GlobalMetadata();
		Set<Regions> replicaSet = gmd.listRegions(tableName);
		
		// Initialize timestamps to all zeros
		for (Regions target : replicaSet) {
			HashMap<String, Long> timesForTarget = new HashMap<String, Long>();
			for (Regions source : replicaSet) {
				timesForTarget.put(source.getName(), 0L);
			}
			timestamps.put(target.getName(), timesForTarget);
		}
		
		// Scan replicas to update latest timestamps from each region
		for (Regions target : replicaSet) {
			AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.standard()
					.withRegion(target)
					.build();
			ScanRequest scanRequest = new ScanRequest()
				    .withTableName(tableName);
			ScanResult scanResult = ddb.scan(scanRequest);
			if (scanResult != null) {
				for (Map<String, AttributeValue> item : scanResult.getItems()){
					Long itemTimestamp = SystemAttributes.getTimestamp(item);
					String itemOrigin = SystemAttributes.getOrigin(item);
					if (itemTimestamp > timestamps.get(target.getName()).get(itemOrigin)) {
						timestamps.get(target.getName()).replace(itemOrigin, itemTimestamp);
					}
				}
			}
		}
		
		return timestamps;
	}

}
