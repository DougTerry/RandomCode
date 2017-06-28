package com.amazonaws.globaltables;

import com.amazonaws.services.dynamodbv2.document.Item;

public class TestData {

	public TestData() {
		// does nothing
	}
	
	public Item getMovieItem(int n) {
		Item item;
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
	
	public Item newMovieItem(String name, int year, String rating, String... fans) {
		Item item = new Item()
				.withString("name", name)
				.withInt("year", year)
				.withString("rating", rating)
				.withStringSet("fans", fans);
		return item;
	}
	
	public void printMovieItem(Item item) {
        System.out.println("-----------");
        if (item != null) {
            System.out.println("name: " + item.getString("name"));
            System.out.println("year: " + item.getInt("year"));
            System.out.println("rating: " + item.getString("rating"));
            System.out.println("fans: " + item.getStringSet("fans"));
    	}
        System.out.println("-----------");
	}

}
