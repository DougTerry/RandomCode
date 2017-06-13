package com.amazonaws.globaltables;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

public class TestData {

	public TestData() {
		// does nothing
	}
	
	public Map<String, AttributeValue> getMovieItem(int n) {
		Map<String, AttributeValue> item;
		switch (n) {
		case 1:
			item = newMovieItem("Bill & Ted's Excellent Adventure", 1989, "****", "Sydney", "Sam");
			break;
		case 2: 
			item = newMovieItem("Airplane", 1980, "**", "Joe");
			break;
		case 3: 
			item = newMovieItem("The King and I", 1957, "***", "Joe");
			break;
		case 4: 
			item = newMovieItem("Schindler's List", 1994, "*****", "Oscar");
			break;
		case 5: 
			item = newMovieItem("The Theory of Everything", 2015, "****", "Eddie");
			break;
		case 6: 
			item = newMovieItem("Out of Africa", 1986, "****", "Joe", "George");
			break;
		case 7: 
			item = newMovieItem("Gone with the Wind", 1940, "*****", "Joe", "George");
			break;
		case 8: 
			item = newMovieItem("The Wizard of Oz", 1940, "***", "Joe", "George");
			break;
		case 9: 
			item = newMovieItem("Slumdog Millionaire", 2009, "****", "Joe", "George");
			break;
		case 10: 
			item = newMovieItem("The Sting", 1974, "****", "Joe", "George", "Margaret");
			break;
		default: 
			item = newMovieItem("Wonder Woman", 2017, "***", "Margaret");
		}
		return item;
	}
	
	public Map<String, AttributeValue> newMovieItem(String name, int year, String rating, String... fans) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("name", new AttributeValue(name));
        item.put("year", new AttributeValue().withN(Integer.toString(year)));
        item.put("rating", new AttributeValue(rating));
        item.put("fans", new AttributeValue().withSS(fans));
        return item;
    }
	
	public void printMovieItem(Map<String, AttributeValue> item) {
        System.out.println("-----------");
        if (item != null) {
            System.out.println("name: " + item.get("name").getS());
            System.out.println("year: " + item.get("year").getN());
            System.out.println("rating: " + item.get("rating").getS());
            System.out.println("fans: " + item.get("fans").getSS());
    	}
        System.out.println("-----------");
	}

}
