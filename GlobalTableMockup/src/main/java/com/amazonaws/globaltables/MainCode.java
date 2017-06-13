/**
 * Code for emulating global table operations
 */
package com.amazonaws.globaltables;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.util.TableUtils;

/**
 * @author terdoug
 *
 */
public class MainCode {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String tableName = "DougsGlobalTable";

        /*
        System.out.println("Running ops on DynamoDB tables in multiple regions...");

        
        // Run ops in California region
        opSuiteOne(Regions.US_WEST_1, tableName);
				
        // Now, let's try a European region!
        System.out.println();
        System.out.println("Now switching to another region.");
        opSuiteTwo(Regions.EU_WEST_1, tableName);
         */
		
        System.out.println("Running global table test...");
		GlobalTableTest test = new GlobalTableTest();
		test.runTest();
        
		System.out.println("Done.");
	}

    private static void opSuiteOne(Regions region, String tableName) {
		try {
			// Create DynamoDB client
			AmazonDynamoDBClientBuilder builder = AmazonDynamoDBClientBuilder.standard();
			AmazonDynamoDB ddb = builder
					.withRegion(region)
					.withCredentials(new ProfileCredentialsProvider("default"))
					.build();

			// Alternatively, just use the default client
			/*
			AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.defaultClient();
			*/

			// Check if DynamoDB is supported in the region
			boolean ok = Region.getRegion(region).isServiceSupported(AmazonDynamoDB.ENDPOINT_PREFIX);
			String is = ok ? "is" : "is not";
			System.out.println("DynamoDB " + is + " supported in region " + region);

            // Create a table with a primary hash key named 'name'
            CreateTableRequest createTableRequest = new CreateTableRequest()
            		.withTableName(tableName)
            		.withKeySchema(new KeySchemaElement()
                		.withAttributeName("name")
                		.withKeyType(KeyType.HASH))
            		.withAttributeDefinitions(new AttributeDefinition()
                		.withAttributeName("name")
                		.withAttributeType(ScalarAttributeType.S))
            		.withProvisionedThroughput(new ProvisionedThroughput()
                		.withReadCapacityUnits(1L)
                		.withWriteCapacityUnits(1L));

            // Create table if it does not exist yet
            TableUtils.createTableIfNotExists(ddb, createTableRequest);
            
            // Alternatively, could call the following, but this will fail if the table already exists
            /*
            try {
            	CreateTableResult createTableResult = ddb.createTable(createTableRequest);
            }
            catch (AmazonServiceException ase) {
            	System.out.println("Table not created because it already exists.");
            }
            */
            
            // Wait for the table to move into ACTIVE state
            try {
				TableUtils.waitUntilActive(ddb, tableName);
			} 
            catch (InterruptedException e) {
				System.out.println("Got interrupted while waiting for table to be created.");
				System.out.println("Exception: " + e.getMessage());
			}
            
            // List tables in the region
            ListTablesResult tables = ddb.listTables();
            System.out.print("Tables:");
            for (String name : tables.getTableNames()) {
            	System.out.print(" " + name);
            }
            System.out.println();

            // Change the provisioned throughput 
            /*
            ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput()
            		.withReadCapacityUnits(5L)
            		.withWriteCapacityUnits(5L);
            ddb.updateTable(tableName, provisionedThroughput);
            */

           // Describe the table
            DescribeTableRequest describeTableRequest = new DescribeTableRequest()
            		.withTableName(tableName);
            TableDescription tableDescription = ddb.describeTable(describeTableRequest).getTable();
            System.out.println("Table Description: ");
            printTableDescription(tableDescription);

            // Add an item
            Map<String, AttributeValue> item = newMovieItem("Bill & Ted's Excellent Adventure", 1989, "****", "Sydney", "Sam");
            PutItemRequest putItemRequest = new PutItemRequest(tableName, item)
            		.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            PutItemResult putItemResult = ddb.putItem(putItemRequest);
            System.out.println("Added item using compacity " + putItemResult.getConsumedCapacity().getCapacityUnits());

            // Add another item
            item = newMovieItem("Airplane", 1980, "*****", "Sydney", "George");
            putItemRequest = new PutItemRequest(tableName, item)
    				.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            putItemResult = ddb.putItem(putItemRequest);
            System.out.println("Added item using compacity " + putItemResult.getConsumedCapacity().getCapacityUnits());
            
            // Get an item
            HashMap<String,AttributeValue> key = new HashMap<String,AttributeValue>();
            key.put("name", new AttributeValue("Airplane"));
            GetItemRequest getItemRequest = new GetItemRequest()
            		.withTableName(tableName)
            		.withKey(key)
            		.withConsistentRead(false)
            		.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            GetItemResult getItemResult = ddb.getItem(getItemRequest);
            System.out.println("Retrieved item using capacity " + getItemResult.getConsumedCapacity().getCapacityUnits());
            printItem(getItemResult.getItem());
            
			// Update an item
            HashMap<String,AttributeValueUpdate> attributeUpdates = new HashMap<String,AttributeValueUpdate>();
            attributeUpdates.put("rating", new AttributeValueUpdate(new AttributeValue("**"), AttributeAction.PUT));
            attributeUpdates.put("fans", new AttributeValueUpdate(new AttributeValue().withSS("Daisy"), AttributeAction.ADD));
            UpdateItemRequest updateItemRequest = new UpdateItemRequest()
            		.withTableName(tableName)
            		.withKey(key)
            		.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
            		.withAttributeUpdates(attributeUpdates);
            UpdateItemResult updateItemResult = ddb.updateItem(updateItemRequest);
            System.out.println("Updated item with name " + key.get("name").getS() + " using capacity " + updateItemResult.getConsumedCapacity().getCapacityUnits());
            
            // Get the updated item
            getItemRequest = new GetItemRequest()
            		.withTableName(tableName)
            		.withKey(key);
            getItemResult = ddb.getItem(getItemRequest);
            System.out.println("New item:");
            printItem(getItemResult.getItem());
            
            // Scan items for movies with a year attribute greater than 1985
            System.out.println("Scanning movies later than 1985...");
            HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();
            Condition condition = new Condition()
                .withComparisonOperator(ComparisonOperator.GT.toString())
                .withAttributeValueList(new AttributeValue()
                		.withN("1985"));
            scanFilter.put("year", condition);
            ScanRequest scanRequest = new ScanRequest(tableName)
            		.withScanFilter(scanFilter)
            		.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            ScanResult scanResult = ddb.scan(scanRequest);
            System.out.println("Scan read " + scanResult.getScannedCount() + " items"
            		+ " using capacity " + scanResult.getConsumedCapacity().getCapacityUnits());
            System.out.println("Scan returned " + scanResult.getCount() + " items");
            for (Map<String, AttributeValue> returnedItem : scanResult.getItems()) {
            	printItem(returnedItem);
            }          
            
            // Delete the table (optionally)
            /*
            ddb.deleteTable(tableName); 
            System.out.println("Deleted " + tableName);
            */
            
            // Shutdown client (though this is not necessary)
			ddb.shutdown();    	
		} 
		catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means a request made it "
					+ "to AWS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());

		} 
		catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with AWS, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
    }
	
    private static void opSuiteTwo(Regions region, String tableName) {
    	try {
    		// Create DynamoDB client
    		AmazonDynamoDBClientBuilder builder = AmazonDynamoDBClientBuilder.standard();
    		AmazonDynamoDB ddb = builder
    				.withRegion(region)
    				.withCredentials(new ProfileCredentialsProvider("default"))
    				.build();

    		// Check if DynamoDB is supported in the region
    		boolean ok = Region.getRegion(region).isServiceSupported(AmazonDynamoDB.ENDPOINT_PREFIX);
    		String is = ok ? "is" : "is not";
    		System.out.println("DynamoDB " + is + " supported in region " + region);

    		// Create a table with a primary hash key named 'name'
    		CreateTableRequest createTableRequest = new CreateTableRequest()
    				.withTableName(tableName)
    				.withKeySchema(new KeySchemaElement()
    						.withAttributeName("name")
    						.withKeyType(KeyType.HASH))
    				.withAttributeDefinitions(new AttributeDefinition()
    						.withAttributeName("name")
    						.withAttributeType(ScalarAttributeType.S))
    				.withProvisionedThroughput(new ProvisionedThroughput()
    						.withReadCapacityUnits(1L)
    						.withWriteCapacityUnits(1L));

    		// Create table if it does not exist yet
    		TableUtils.createTableIfNotExists(ddb, createTableRequest);

    		// Wait for the table to move into ACTIVE state
    		try {
    			TableUtils.waitUntilActive(ddb, tableName);
    		} 
    		catch (InterruptedException e) {
    			System.out.println("Got interrupted while waiting for table to be created.");
    			System.out.println("Exception: " + e.getMessage());
    		}

    		// List tables in the region
    		ListTablesResult tables = ddb.listTables();
    		System.out.print("Tables:");
    		for (String name : tables.getTableNames()) {
    			System.out.print(" " + name);
    		}
    		System.out.println();

    		// Describe the table
    		DescribeTableRequest describeTableRequest = new DescribeTableRequest()
    				.withTableName(tableName);
    		TableDescription tableDescription = ddb.describeTable(describeTableRequest).getTable();
    		System.out.println("Table Description: ");
    		printTableDescription(tableDescription);

    		// Add an item
    		Map<String, AttributeValue> item = newMovieItem("Wonder Woman", 2017, "***", "Margaret");
            PutItemRequest putItemRequest = new PutItemRequest(tableName, item)
            		.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            PutItemResult putItemResult = ddb.putItem(putItemRequest);
            System.out.println("Added item using compacity " + putItemResult.getConsumedCapacity().getCapacityUnits());

    		// Get the item
    		HashMap<String, AttributeValue> key = new HashMap<String,AttributeValue>();
    		key.put("name", new AttributeValue("Wonder Woman"));
    		GetItemRequest getItemRequest = new GetItemRequest()
    				.withTableName(tableName)
    				.withKey(key)
    				.withConsistentRead(true)
    				.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
    		GetItemResult getItemResult = ddb.getItem(getItemRequest);
            System.out.println("Retrieved item using capacity " + getItemResult.getConsumedCapacity().getCapacityUnits());
    		printItem(getItemResult.getItem());

    		// Shutdown client (though this is not necessary)
    		ddb.shutdown();    	
    	} 
    	catch (AmazonServiceException ase) {
    		System.out.println("Caught an AmazonServiceException, which means a request made it "
    				+ "to AWS, but was rejected with an error response for some reason.");
    		System.out.println("Error Message:    " + ase.getMessage());
    		System.out.println("HTTP Status Code: " + ase.getStatusCode());
    		System.out.println("AWS Error Code:   " + ase.getErrorCode());
    		System.out.println("Error Type:       " + ase.getErrorType());
    		System.out.println("Request ID:       " + ase.getRequestId());

    	} 
    	catch (AmazonClientException ace) {
    		System.out.println("Caught an AmazonClientException, which means the client encountered "
    				+ "a serious internal problem while trying to communicate with AWS, "
    				+ "such as not being able to access the network.");
    		System.out.println("Error Message: " + ace.getMessage());
    	}
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
    
    private static void printItem (Map<String,AttributeValue> item) {
    	if (item != null) {
            System.out.println("-----------");
    		Set<String> keys = item.keySet();
    		for (String key : keys) {
    			System.out.format("%s: %s\n",
    					key, item.get(key).toString());
    		}
            System.out.println("-----------");
    	}
    }
	
	private static Map<String, AttributeValue> newMovieItem(String name, int year, String rating, String... fans) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("name", new AttributeValue(name));
        item.put("year", new AttributeValue().withN(Integer.toString(year)));
        item.put("rating", new AttributeValue(rating));
        item.put("fans", new AttributeValue().withSS(fans));
        return item;
    }
}
