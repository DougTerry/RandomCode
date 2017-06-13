package com.amazonaws.globaltables;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
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
	
	public static final Regions MASTER_REGION = Regions.EU_WEST_1;  // California
	public static final Regions LOCAL_REGION = Regions.US_WEST_1;  // Dublin
	public static final Regions OTHER_REGION = Regions.AP_SOUTHEAST_2;  // Sydney
	
	public GlobalTableTest() {
	}
	
	public void runTest() {
		GlobalMetadata gmd = new GlobalMetadata();
		TestData data = new TestData();
		GlobalRequestRouter grr = new GlobalRequestRouter(TABLE_NAME, LOCAL_REGION);
		
		AmazonDynamoDB ddbMaster = AmazonDynamoDBClientBuilder.standard()
				.withRegion(MASTER_REGION)
				.build();
		AmazonDynamoDB ddbLocal = AmazonDynamoDBClientBuilder.standard()
				.withRegion(LOCAL_REGION)
				.build();
		AmazonDynamoDB ddbOther = AmazonDynamoDBClientBuilder.standard()
				.withRegion(OTHER_REGION)
				.build();
		
		Map<String, AttributeValue> item;
		GetItemRequest getItemRequest;
		GetItemResult getItemResult;
		ConsistentPutItemRequest putItemRequest;
		PutItemResult putItemResult;
		ConsistentUpdateItemRequest updateItemRequest;
		UpdateItemResult updateItemResult;
		DescribeTableRequest describeTableRequest;
		TableDescription tableDescription;
		HashMap<String,AttributeValue> key;
		HashMap<String,AttributeValueUpdate> attributeUpdates;
		
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
		List<String> regionSet = gmd.listRegions(TABLE_NAME);
		System.out.print("Regions:");			
		for (String region : regionSet) {
			System.out.print(" " + region);						
		}
		System.out.println();

        // Describe the tables in each region
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
        
        // Write 2 items with strong consistency
        item = data.getMovieItem(1);
        putItemRequest = (ConsistentPutItemRequest) new ConsistentPutItemRequest()
        		.withConsistentWrite(true)
        		.withTableName(TABLE_NAME)
        		.withItem(item);
        putItemResult = grr.putItem(putItemRequest);
        System.out.println("Strong write of " + item.get(TABLE_KEY).getS());
        
        item = data.getMovieItem(2);
        putItemRequest = (ConsistentPutItemRequest) new ConsistentPutItemRequest()
        		.withConsistentWrite(true)
        		.withTableName(TABLE_NAME)
        		.withItem(item);
        putItemResult = grr.putItem(putItemRequest);
        System.out.println("Strong write of " + item.get(TABLE_KEY).getS());
        
        // Write 3 items with eventual consistency
        item = data.getMovieItem(3);
        putItemRequest = (ConsistentPutItemRequest) new ConsistentPutItemRequest()
        		.withConsistentWrite(false)
        		.withTableName(TABLE_NAME)
        		.withItem(item);
        putItemResult = grr.putItem(putItemRequest);
        System.out.println("Eventual write of " + item.get(TABLE_KEY).getS());
        
        item = data.getMovieItem(4);
        putItemRequest = (ConsistentPutItemRequest) new ConsistentPutItemRequest()
        		.withConsistentWrite(false)
        		.withTableName(TABLE_NAME)
        		.withItem(item);
        putItemResult = grr.putItem(putItemRequest);
        System.out.println("Eventual write of " + item.get(TABLE_KEY).getS());
        
        item = data.getMovieItem(5);
        putItemRequest = (ConsistentPutItemRequest) new ConsistentPutItemRequest()
        		.withConsistentWrite(false)
        		.withTableName(TABLE_NAME)
        		.withItem(item);
        putItemResult = grr.putItem(putItemRequest);
        System.out.println("Eventual write of " + item.get(TABLE_KEY).getS());
        
        // Read item with strong consistency
        item = data.getMovieItem(2);
        key = new HashMap<String,AttributeValue>();
        key.put(TABLE_KEY, item.get(TABLE_KEY));
        getItemRequest = new GetItemRequest()
        		.withTableName(TABLE_NAME)
        		.withKey(key)
        		.withConsistentRead(true);
        getItemResult = grr.getItem(getItemRequest);
        System.out.println("Retrieved item:");
        data.printMovieItem(getItemResult.getItem());
        
        // Read item with eventual consistency that was written locally
        item = data.getMovieItem(4);
        key = new HashMap<String,AttributeValue>();
        key.put(TABLE_KEY, item.get(TABLE_KEY));
        getItemRequest = new GetItemRequest()
        		.withTableName(TABLE_NAME)
        		.withKey(key)
        		.withConsistentRead(false);
        getItemResult = grr.getItem(getItemRequest);
        System.out.println("Retrieved item:");
        data.printMovieItem(getItemResult.getItem());
        
        // Delete item from local region so that the next read fails 
        // Note: The item will be restored during the next replication
        item = data.getMovieItem(1);
        Table table = new DynamoDB(LOCAL_REGION).getTable(TABLE_NAME);
        String itemName = item.get(TABLE_KEY).getS();
        table.deleteItem(TABLE_KEY, itemName);
        
        // Read item with eventual consistency that has not yet been replicated locally
        key = new HashMap<String,AttributeValue>();
        key.put(TABLE_KEY, item.get(TABLE_KEY));
        getItemRequest = new GetItemRequest()
        		.withTableName(TABLE_NAME)
        		.withKey(key)
        		.withConsistentRead(false);
        getItemResult = grr.getItem(getItemRequest);
        if (getItemResult.getItem() == null) {
        	System.out.println("Unable to eventually read " + item.get(TABLE_KEY).getS());
        } else {
        	System.out.println("Retrieved item:");
            data.printMovieItem(getItemResult.getItem());
        }
        
        // Try to read item that does not exist
        item = data.getMovieItem(10);
        key = new HashMap<String,AttributeValue>();
        key.put(TABLE_KEY, item.get(TABLE_KEY));
        getItemRequest = new GetItemRequest()
        		.withTableName(TABLE_NAME)
        		.withKey(key)
        		.withConsistentRead(true);
        getItemResult = grr.getItem(getItemRequest);
        if (getItemResult.getItem() == null) {
        	System.out.println("Unable to strongly read " + item.get(TABLE_KEY).getS());
        } else {
        	System.out.println("Retrieved item:");
            data.printMovieItem(getItemResult.getItem());
        }
        
        // Update an item with eventual consistency
        item = data.getMovieItem(4);
        key = new HashMap<String,AttributeValue>();
        key.put(TABLE_KEY, item.get(TABLE_KEY));
        attributeUpdates = new HashMap<String,AttributeValueUpdate>();
        attributeUpdates.put("rating", new AttributeValueUpdate(new AttributeValue("*"), AttributeAction.PUT));
        attributeUpdates.put("fans", new AttributeValueUpdate(new AttributeValue().withSS("Daisy"), AttributeAction.ADD));
        updateItemRequest = (ConsistentUpdateItemRequest) new ConsistentUpdateItemRequest()
        		.withConsistentWrite(false)
        		.withTableName(TABLE_NAME)
        		.withKey(key)
        		.withAttributeUpdates(attributeUpdates);
        updateItemResult = grr.updateItem(updateItemRequest);
        System.out.println("Eventual update of " + item.get(TABLE_KEY).getS());
                
        // Read updated item with eventual consistency 
        getItemRequest = new GetItemRequest()
        		.withTableName(TABLE_NAME)
        		.withKey(key)
        		.withConsistentRead(false);
        getItemResult = grr.getItem(getItemRequest);
        System.out.println("Updated item:");
        data.printMovieItem(getItemResult.getItem());
        
        // Update an item with strong consistency
        item = data.getMovieItem(2);
        key = new HashMap<String,AttributeValue>();
        key.put(TABLE_KEY, item.get(TABLE_KEY));
        attributeUpdates = new HashMap<String,AttributeValueUpdate>();
        attributeUpdates.put("rating", new AttributeValueUpdate(new AttributeValue("*"), AttributeAction.PUT));
        attributeUpdates.put("fans", new AttributeValueUpdate(new AttributeValue().withSS("Daisy"), AttributeAction.ADD));
        updateItemRequest = (ConsistentUpdateItemRequest) new ConsistentUpdateItemRequest()
        		.withConsistentWrite(true)
        		.withTableName(TABLE_NAME)
        		.withKey(key)
        		.withAttributeUpdates(attributeUpdates);
        updateItemResult = grr.updateItem(updateItemRequest);
        System.out.println("Strong consistency update of " + item.get(TABLE_KEY).getS());
        
        // Read updated item with strong consistency
        getItemRequest = new GetItemRequest()
        		.withTableName(TABLE_NAME)
        		.withKey(key)
        		.withConsistentRead(true);
        getItemResult = grr.getItem(getItemRequest);
        System.out.println("Updated item:");
        data.printMovieItem(getItemResult.getItem());
        
        // Do conflicting update to an item with eventual consistency
        item = data.getMovieItem(2);
        key = new HashMap<String,AttributeValue>();
        key.put(TABLE_KEY, item.get(TABLE_KEY));
        attributeUpdates = new HashMap<String,AttributeValueUpdate>();
        attributeUpdates.put("rating", new AttributeValueUpdate(new AttributeValue("*****"), AttributeAction.PUT));
        updateItemRequest = (ConsistentUpdateItemRequest) new ConsistentUpdateItemRequest()
        		.withConsistentWrite(false)
        		.withTableName(TABLE_NAME)
        		.withKey(key)
        		.withAttributeUpdates(attributeUpdates);
        updateItemResult = grr.updateItem(updateItemRequest);
        System.out.println("Conflicting eventual consistency update of " + item.get(TABLE_KEY).getS());
        
        // Read updated item with eventual consistency 
        getItemRequest = new GetItemRequest()
        		.withTableName(TABLE_NAME)
        		.withKey(key)
        		.withConsistentRead(false);
        getItemResult = grr.getItem(getItemRequest);
        System.out.println("Updated item:");
        data.printMovieItem(getItemResult.getItem());
        
        // Replicate items
        ReplicationEngine re = new ReplicationEngine();
        int num;
        System.out.println("Replicating items...");
        num = re.pullItems(TABLE_NAME, OTHER_REGION, MASTER_REGION);
        System.out.println(num + " items replicated from " + MASTER_REGION + " to " + OTHER_REGION);
        num = re.pullItems(TABLE_NAME, OTHER_REGION, LOCAL_REGION);
        System.out.println(num + " items replicated from " + LOCAL_REGION + " to " + OTHER_REGION);
        num = re.pullItems(TABLE_NAME, OTHER_REGION, LOCAL_REGION);
        System.out.println(num + " items replicated from " + LOCAL_REGION + " to " + OTHER_REGION);
        num = re.pullItems(TABLE_NAME, LOCAL_REGION, MASTER_REGION);
        System.out.println(num + " items replicated from " + MASTER_REGION + " to " + LOCAL_REGION);
        num = re.pullItems(TABLE_NAME, LOCAL_REGION, OTHER_REGION);
        System.out.println(num + " items replicated from " + OTHER_REGION + " to " + LOCAL_REGION);
        num = re.pullItems(TABLE_NAME, MASTER_REGION, LOCAL_REGION);
        System.out.println(num + " items replicated from " + LOCAL_REGION + " to " + MASTER_REGION);
        num = re.pullItems(TABLE_NAME, MASTER_REGION, OTHER_REGION);
        System.out.println(num + " items replicated from " + OTHER_REGION + " to " + MASTER_REGION);
        System.out.println("Completed replication.");
        System.out.println();
        
        // Read updated item with eventual consistency 
        item = data.getMovieItem(2);
        System.out.println("Eventual consistency read of item " + item.get(TABLE_KEY).getS());
        getItemRequest = new GetItemRequest()
        		.withTableName(TABLE_NAME)
        		.withKey(key)
        		.withConsistentRead(false);
        getItemResult = grr.getItem(getItemRequest);
        System.out.println("Retrieved item:");
        data.printMovieItem(getItemResult.getItem());
        
        // Do another update to an item with eventual consistency
        item = data.getMovieItem(2);
        key = new HashMap<String,AttributeValue>();
        key.put(TABLE_KEY, item.get(TABLE_KEY));
        attributeUpdates = new HashMap<String,AttributeValueUpdate>();
        attributeUpdates.put("rating", new AttributeValueUpdate(new AttributeValue("*****"), AttributeAction.PUT));
        updateItemRequest = (ConsistentUpdateItemRequest) new ConsistentUpdateItemRequest()
        		.withConsistentWrite(false)
        		.withTableName(TABLE_NAME)
        		.withKey(key)
        		.withAttributeUpdates(attributeUpdates);
        updateItemResult = grr.updateItem(updateItemRequest);
        System.out.println("Non-conflicting eventual update of " + item.get(TABLE_KEY).getS());
        
        // Read updated item with eventual consistency 
        getItemRequest = new GetItemRequest()
        		.withTableName(TABLE_NAME)
        		.withKey(key)
        		.withConsistentRead(false);
        getItemResult = grr.getItem(getItemRequest);
        System.out.println("Updated item:");
        data.printMovieItem(getItemResult.getItem());
        
        // Re-replicate to master
        System.out.println("Replicating update to master region...");
        num = re.pullItems(TABLE_NAME, MASTER_REGION, LOCAL_REGION);
        System.out.println(num + " items replicated from " + LOCAL_REGION + " to " + MASTER_REGION);

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
