package com.amazonaws.globaltables;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.util.TableUtils;

public class GlobalTableTest {

	public final static String TABLE_NAME = "GlobalMovies";
	
	public final static String TABLE_KEY = "name";
	
	public static final Regions MASTER_REGION = Regions.EU_CENTRAL_1;  // Frankfurt
	public static final Regions LOCAL_REGION = Regions.US_WEST_1;  // California
	public static final Regions OTHER_REGION = Regions.AP_SOUTHEAST_2;  // Sydney
	
	public GlobalTableTest() {
	}
	
	public void runTest() {
		GlobalMetadata gmd = new GlobalMetadata();
		Lease masterLease = new Lease();
		TestData data = new TestData();
		GlobalRequestRouter grr;
		long startTime, elapsedTime;
		
		AmazonDynamoDB ddbMaster = AmazonDynamoDBClientBuilder.standard()
				.withRegion(MASTER_REGION)
				.build();
		AmazonDynamoDB ddbLocal = AmazonDynamoDBClientBuilder.standard()
				.withRegion(LOCAL_REGION)
				.build();
		AmazonDynamoDB ddbOther = AmazonDynamoDBClientBuilder.standard()
				.withRegion(OTHER_REGION)
				.build();
		
		Item item;
		Item itemRead;
		DescribeTableRequest describeTableRequest;
		TableDescription tableDescription;
		GetItemSpec getItemSpec;
		ConsistentPutItemSpec putItemSpec;
		ConsistentUpdateItemSpec updateItemSpec;
		
		// Create global table in metadata
		boolean done = gmd.createTable(TABLE_NAME, MASTER_REGION);
		if (done) {
			System.out.println("Created new global table named " + TABLE_NAME);
		} else {
			System.out.println("Global table named " + TABLE_NAME + " already exists");	
		}
		
		// Create actual table in region if necessary
		CreateTableRequest createTableRequest = new CreateTableRequest()
        		.withTableName(TABLE_NAME)
        		.withKeySchema(new KeySchemaElement()
            		.withAttributeName(TABLE_KEY)
            		.withKeyType(KeyType.HASH))
        		.withAttributeDefinitions(new AttributeDefinition()
            		.withAttributeName(TABLE_KEY)
            		.withAttributeType(ScalarAttributeType.S))
				.withProvisionedThroughput(new ProvisionedThroughput()
					.withReadCapacityUnits(1L)
					.withWriteCapacityUnits(1L));
        TableUtils.createTableIfNotExists(ddbMaster, createTableRequest);
		
		// Get master
		System.out.println("Master region is " + gmd.getMaster(TABLE_NAME));			

		// Add a second region
		gmd.addRegion(TABLE_NAME, LOCAL_REGION);
        TableUtils.createTableIfNotExists(ddbLocal, createTableRequest);
		System.out.println("Added region " + LOCAL_REGION);			
				
		// Add a third region
		gmd.addRegion(TABLE_NAME, OTHER_REGION);
        TableUtils.createTableIfNotExists(ddbOther, createTableRequest);
		System.out.println("Added region " + OTHER_REGION);			
		
		// Show all regions
		Set<Regions> regionSet = gmd.listRegions(TABLE_NAME);
		System.out.print("Regions:");			
		for (Regions region : regionSet) {
			System.out.print(" " + region);						
		}
		System.out.println();

        // Describe the tables in each region
        System.out.println();
        describeTableRequest = new DescribeTableRequest()
        		.withTableName(TABLE_NAME);
        tableDescription = ddbMaster.describeTable(describeTableRequest).getTable();
        System.out.println("Table in region " + MASTER_REGION + ": ");
        printTableDescription(tableDescription);
		
        describeTableRequest = new DescribeTableRequest()
        		.withTableName(TABLE_NAME);
        tableDescription = ddbLocal.describeTable(describeTableRequest).getTable();
        System.out.println("Table in region " + LOCAL_REGION + ": ");
        printTableDescription(tableDescription);
		
        describeTableRequest = new DescribeTableRequest()
        		.withTableName(TABLE_NAME);
        tableDescription = ddbOther.describeTable(describeTableRequest).getTable();
        System.out.println("Table in region " + OTHER_REGION + ": ");
        printTableDescription(tableDescription);
        
        // Delete an item from all regions so that we can put it again
        item = data.getMovieItem(1);
        String itemName = item.getString(TABLE_KEY);
        Table table = new Table(ddbMaster, TABLE_NAME);
        table.deleteItem(TABLE_KEY, itemName);
        table = new Table(ddbLocal, TABLE_NAME);
        table.deleteItem(TABLE_KEY, itemName);
        table = new Table(ddbOther, TABLE_NAME);
        table.deleteItem(TABLE_KEY, itemName);
   
        // Create Global Request Router to use for all reads and writes
		masterLease.acquire(MASTER_REGION);
        grr = new GlobalRequestRouter(TABLE_NAME, LOCAL_REGION, masterLease);
        System.out.println();
        System.out.println("Using Global Request Router in region " + LOCAL_REGION);
        System.out.println();

        // Write 2 items with strong consistency
        item = data.getMovieItem(1);
        startTime = System.currentTimeMillis();
        grr.putItem(item);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("Strong consistency write of " + item.get(TABLE_KEY));
        System.out.println("     Latency = " + elapsedTime + " ms.");
        
        item = data.getMovieItem(2);
        putItemSpec = (ConsistentPutItemSpec) new ConsistentPutItemSpec()
        		.withConsistentWrite(true)
        		.withItem(item);
        startTime = System.currentTimeMillis();
        grr.putItem(putItemSpec);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("Strong consistency write of " + item.get(TABLE_KEY));
        System.out.println("     Latency = " + elapsedTime + " ms.");
        
        // Write 3 items with eventual consistency
        item = data.getMovieItem(3);
        putItemSpec = (ConsistentPutItemSpec) new ConsistentPutItemSpec()
        		.withConsistentWrite(false)
        		.withItem(item);
        startTime = System.currentTimeMillis();
        grr.putItem(putItemSpec);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("Eventual consistency write of " + item.get(TABLE_KEY));
        System.out.println("     Latency = " + elapsedTime + " ms.");
        
        item = data.getMovieItem(4);
        putItemSpec = (ConsistentPutItemSpec) new ConsistentPutItemSpec()
        		.withConsistentWrite(false)
        		.withItem(item);
        startTime = System.currentTimeMillis();
        grr.putItem(putItemSpec);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("Eventual consistency write of " + item.get(TABLE_KEY));
        System.out.println("     Latency = " + elapsedTime + " ms.");
        
        item = data.getMovieItem(5);
        putItemSpec = (ConsistentPutItemSpec) new ConsistentPutItemSpec()
        		.withConsistentWrite(false)
        		.withItem(item);
        startTime = System.currentTimeMillis();
        grr.putItem(putItemSpec);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("Eventual consistency write of " + item.get(TABLE_KEY));
        System.out.println("     Latency = " + elapsedTime + " ms.");
        
        // Read item with strong consistency
        item = data.getMovieItem(2);
        getItemSpec = new GetItemSpec()
        		.withPrimaryKey(TABLE_KEY, item.getString(TABLE_KEY))
        		.withConsistentRead(true);
        startTime = System.currentTimeMillis();
        itemRead = grr.getItem(getItemSpec);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println();
        System.out.println("Strong consistency read of " + item.get(TABLE_KEY));
        System.out.println("     Latency = " + elapsedTime + " ms.");
        System.out.println("Retrieved item:");
        data.printMovieItem(itemRead);
        
        // Read item with eventual consistency that was written locally
        item = data.getMovieItem(4);
        startTime = System.currentTimeMillis();
        itemRead = grr.getItem(TABLE_KEY, item.getString(TABLE_KEY));
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println();
        System.out.println("Eventual consistency read of " + item.get(TABLE_KEY));
        System.out.println("     Latency = " + elapsedTime + " ms.");
        System.out.println("Retrieved item:");
        data.printMovieItem(itemRead);
        
        // Read item with eventual consistency that has not yet been replicated locally
        item = data.getMovieItem(1);
        getItemSpec = new GetItemSpec()
        		.withPrimaryKey(TABLE_KEY, item.getString(TABLE_KEY))
        		.withConsistentRead(false);
        startTime = System.currentTimeMillis();
        itemRead = grr.getItem(getItemSpec);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println();
        if (itemRead == null) {
        	System.out.println("Unable to read " + item.get(TABLE_KEY) + " with eventual consistency");
        } else {
        	System.out.println("Unexpectedly retrieved item:");
            data.printMovieItem(itemRead);
        }
        System.out.println("     Latency = " + elapsedTime + " ms.");
        
        // Try to read item that does not exist
        item = data.getMovieItem(10);
        getItemSpec = new GetItemSpec()
        		.withPrimaryKey(TABLE_KEY, item.getString(TABLE_KEY))
        		.withConsistentRead(true);
        startTime = System.currentTimeMillis();
        itemRead = grr.getItem(getItemSpec);
        elapsedTime = System.currentTimeMillis() - startTime;
        if (itemRead == null) {
        	System.out.println("Unable to read " + item.get(TABLE_KEY) + " with strong consistency");
        } else {
        	System.out.println("Unexpectedly retrieved item:");
            data.printMovieItem(itemRead);
        }
        System.out.println("     Latency = " + elapsedTime + " ms.");
        
        // Update an item with eventual consistency
        item = data.getMovieItem(4);
    	AttributeUpdate updateRating = new AttributeUpdate("rating").put("***");
    	AttributeUpdate updateFans = new AttributeUpdate("fans").addElements("Daisy");
    	updateItemSpec = (ConsistentUpdateItemSpec) new ConsistentUpdateItemSpec()
				.withConsistentWrite(false)
				.withPrimaryKey(TABLE_KEY, item.getString(TABLE_KEY))
				.withAttributeUpdate(updateRating, updateFans);
        startTime = System.currentTimeMillis();
    	UpdateItemOutcome outcome = grr.updateItem(updateItemSpec);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println();
        System.out.println("Eventual consistency update of " + item.get(TABLE_KEY));
        System.out.println("     Latency = " + elapsedTime + " ms.");
                
        // Read updated item with eventual consistency 
        getItemSpec = new GetItemSpec()
        		.withPrimaryKey(TABLE_KEY, item.getString(TABLE_KEY))
        		.withConsistentRead(false);
        startTime = System.currentTimeMillis();
        itemRead = grr.getItem(getItemSpec);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("Updated item:");
        data.printMovieItem(itemRead);
        
        // Update an item with strong consistency
        item = data.getMovieItem(2);
    	updateRating = new AttributeUpdate("rating").put("*");
    	updateFans = new AttributeUpdate("fans").addElements("Daisy");
    	updateItemSpec = (ConsistentUpdateItemSpec) new ConsistentUpdateItemSpec()
				.withConsistentWrite(true)
				.withPrimaryKey(TABLE_KEY, item.getString(TABLE_KEY))
				.withAttributeUpdate(updateRating, updateFans);
        startTime = System.currentTimeMillis();
        grr.updateItem(updateItemSpec);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println();
        System.out.println("Strong consistency update of " + item.get(TABLE_KEY));
        System.out.println("     Latency = " + elapsedTime + " ms.");
        
        // Read updated item with strong consistency
        getItemSpec = new GetItemSpec()
        		.withPrimaryKey(TABLE_KEY, item.getString(TABLE_KEY))
        		.withConsistentRead(true);
        startTime = System.currentTimeMillis();
        itemRead = grr.getItem(getItemSpec);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("Updated item:");
        data.printMovieItem(itemRead);
        
        // Do conflicting update to an item with eventual consistency
    	updateRating = new AttributeUpdate("rating").put("*****");
    	updateFans = new AttributeUpdate("fans").addElements("Lance")
    			.removeElements("Daisy");
    	updateItemSpec = (ConsistentUpdateItemSpec) new ConsistentUpdateItemSpec()
				.withConsistentWrite(false)
				.withPrimaryKey(TABLE_KEY, item.getString(TABLE_KEY))
				.withAttributeUpdate(updateRating, updateFans);
        startTime = System.currentTimeMillis();
        grr.updateItem(updateItemSpec);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println();
        System.out.println("Conflicting eventual consistency update of " + item.get(TABLE_KEY));
        System.out.println("     Latency = " + elapsedTime + " ms.");
        
        // Read updated item with eventual consistency 
        getItemSpec = new GetItemSpec()
        		.withPrimaryKey(TABLE_KEY, item.getString(TABLE_KEY))
        		.withConsistentRead(false);
        startTime = System.currentTimeMillis();
        itemRead = grr.getItem(getItemSpec);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("Updated item:");
        data.printMovieItem(itemRead);
        
        // Replicate items
        System.out.println();
        ReplicationEngine re = new ReplicationEngine();
        re.generateTimestamps(TABLE_NAME);
        int num;
        System.out.println("Replicating items...");
        System.out.println();
        System.out.println("Replication from " + MASTER_REGION + " to " + OTHER_REGION);
        startTime = System.currentTimeMillis();
        num = re.pullItems(TABLE_NAME, OTHER_REGION, MASTER_REGION);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("     " + num + " items replicated");
        System.out.println("     Latency = " + elapsedTime + " ms.");
        System.out.println();
        System.out.println("Replication from " + LOCAL_REGION + " to " + OTHER_REGION);
        startTime = System.currentTimeMillis();
        num = re.pullItems(TABLE_NAME, OTHER_REGION, LOCAL_REGION);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("     " + num + " items replicated");
        System.out.println("     Latency = " + elapsedTime + " ms.");
        System.out.println();
        System.out.println("Replication from " + MASTER_REGION + " to " + LOCAL_REGION);
        startTime = System.currentTimeMillis();
        num = re.pullItems(TABLE_NAME, LOCAL_REGION, MASTER_REGION);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("     " + num + " items replicated");
        System.out.println("     Latency = " + elapsedTime + " ms.");
        System.out.println();
        System.out.println("Replication from " + OTHER_REGION + " to " + LOCAL_REGION);
        startTime = System.currentTimeMillis();
        num = re.pullItems(TABLE_NAME, LOCAL_REGION, OTHER_REGION);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("     " + num + " items replicated");
        System.out.println("     Latency = " + elapsedTime + " ms.");
        System.out.println();
        System.out.println("Replication from " + LOCAL_REGION + " to " + MASTER_REGION);
        startTime = System.currentTimeMillis();
        num = re.pullItems(TABLE_NAME, MASTER_REGION, LOCAL_REGION);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("     " + num + " items replicated");
        System.out.println("     Latency = " + elapsedTime + " ms.");
        System.out.println();
        System.out.println("Replication from " + OTHER_REGION + " to " + MASTER_REGION);
        startTime = System.currentTimeMillis();
        num = re.pullItems(TABLE_NAME, MASTER_REGION, OTHER_REGION);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("     " + num + " items replicated");
        System.out.println("     Latency = " + elapsedTime + " ms.");
        System.out.println();
        System.out.println("Completed replication.");
        System.out.println();
        
        // Replicate again to show that nothing gets sent
        System.out.println("Replicating items again...");
        System.out.println();
        System.out.println("Replication from " + MASTER_REGION + " to " + OTHER_REGION);
        startTime = System.currentTimeMillis();
        num = re.pullItems(TABLE_NAME, OTHER_REGION, MASTER_REGION);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("     " + num + " items replicated");
        System.out.println("     Latency = " + elapsedTime + " ms.");
        System.out.println();
        System.out.println("Replication from " + LOCAL_REGION + " to " + OTHER_REGION);
        startTime = System.currentTimeMillis();
        num = re.pullItems(TABLE_NAME, OTHER_REGION, LOCAL_REGION);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("     " + num + " items replicated");
        System.out.println("     Latency = " + elapsedTime + " ms.");
        System.out.println();
        System.out.println("Replication from " + MASTER_REGION + " to " + LOCAL_REGION);
        startTime = System.currentTimeMillis();
        num = re.pullItems(TABLE_NAME, LOCAL_REGION, MASTER_REGION);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("     " + num + " items replicated");
        System.out.println("     Latency = " + elapsedTime + " ms.");
        System.out.println();
        System.out.println("Replication from " + OTHER_REGION + " to " + LOCAL_REGION);
        startTime = System.currentTimeMillis();
        num = re.pullItems(TABLE_NAME, LOCAL_REGION, OTHER_REGION);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("     " + num + " items replicated");
        System.out.println("     Latency = " + elapsedTime + " ms.");
        System.out.println();
        System.out.println("Replication from " + LOCAL_REGION + " to " + MASTER_REGION);
        startTime = System.currentTimeMillis();
        num = re.pullItems(TABLE_NAME, MASTER_REGION, LOCAL_REGION);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("     " + num + " items replicated");
        System.out.println("     Latency = " + elapsedTime + " ms.");
        System.out.println();
        System.out.println("Replication from " + OTHER_REGION + " to " + MASTER_REGION);
        startTime = System.currentTimeMillis();
        num = re.pullItems(TABLE_NAME, MASTER_REGION, OTHER_REGION);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("     " + num + " items replicated");
        System.out.println("     Latency = " + elapsedTime + " ms.");
        System.out.println();
        System.out.println("Completed replication.");
        
        // Renew master lease just for fun
        boolean renewed = masterLease.renew(MASTER_REGION);
        if (renewed) {
        	System.out.println();
        	System.out.println("Renewed master lease for " + MASTER_REGION);
        }
        
        // Read updated item with eventual consistency 
        item = data.getMovieItem(2);
        System.out.println();
        System.out.println("Eventual consistency read of item " + item.get(TABLE_KEY));
        getItemSpec = new GetItemSpec()
        		.withPrimaryKey(TABLE_KEY, item.getString(TABLE_KEY))
        		.withConsistentRead(false);
        startTime = System.currentTimeMillis();
        itemRead = grr.getItem(getItemSpec);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("     Latency = " + elapsedTime + " ms.");
        System.out.println("Winning item:");
        data.printMovieItem(itemRead);
        
        // Do another update to an item with eventual consistency
    	updateRating = new AttributeUpdate("rating").put("*****");
    	updateFans = new AttributeUpdate("fans").addElements("Lance");
    	updateItemSpec = (ConsistentUpdateItemSpec) new ConsistentUpdateItemSpec()
				.withConsistentWrite(false)
				.withPrimaryKey(TABLE_KEY, item.getString(TABLE_KEY))
				.withAttributeUpdate(updateRating, updateFans);
        startTime = System.currentTimeMillis();
        grr.updateItem(updateItemSpec);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println();
        System.out.println("Non-conflicting eventual consistency update of " + item.get(TABLE_KEY));
        System.out.println("     Latency = " + elapsedTime + " ms.");
        
        // Read updated item with eventual consistency 
        getItemSpec = new GetItemSpec()
        		.withPrimaryKey(TABLE_KEY, item.getString(TABLE_KEY))
        		.withConsistentRead(false);
        startTime = System.currentTimeMillis();
        itemRead = grr.getItem(getItemSpec);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("Updated item:");
        data.printMovieItem(itemRead);
        
        // Re-replicate to master
        System.out.println();
        System.out.println("Replication from " + LOCAL_REGION + " to " + MASTER_REGION);
        startTime = System.currentTimeMillis();
        num = re.pullItems(TABLE_NAME, MASTER_REGION, LOCAL_REGION);
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("     " + num + " items replicated");
        System.out.println("     Latency = " + elapsedTime + " ms.");
        
        // Replicate global metadata in multiple regions as its own global table
        System.out.println();
        gmd.bootstrapMetadata();
        System.out.println("Replication of metadata items from " + gmd.getMetadataMaster() 
    		+ " to " + gmd.getMetadataSecondary());
        startTime = System.currentTimeMillis();
        num = gmd.replicateMetadata();
        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("     " + num + " items replicated");
        System.out.println("     Latency = " + elapsedTime + " ms.");
        System.out.println();
	}
	
	private static void printTableDescription(TableDescription td) {
        if (td != null) {
            System.out.println("-----------");
        	System.out.format("Table name  : %s\n",
                  td.getTableName());
            System.out.format("Table ARN   : %s\n",
                  td.getTableArn());
            System.out.format("Status      : %s\n",
                  td.getTableStatus());
            System.out.format("Item count  : %d\n",
                  td.getItemCount().longValue());
            System.out.format("Size (bytes): %d\n",
                  td.getTableSizeBytes().longValue());

            ProvisionedThroughputDescription throughput_info =
               td.getProvisionedThroughput();
            System.out.println("Throughput");
            System.out.format("  Read Capacity : %d\n",
                  throughput_info.getReadCapacityUnits().longValue());
            System.out.format("  Write Capacity: %d\n",
                  throughput_info.getWriteCapacityUnits().longValue());

            List<AttributeDefinition> attributes =
               td.getAttributeDefinitions();
            System.out.println("Attributes");
            for (AttributeDefinition a : attributes) {
                System.out.format("  %s (%s)\n",
                      a.getAttributeName(), a.getAttributeType());
            }
            System.out.println("-----------");
        }    	
    }
    	
}
