package com.amazonaws.globaltables;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class VersionVector {

	/*
	 * A version vector is a mapping from replica names to counts of the number of updates made by each replica.
	 * A missing entry implies a count of zero.
	 */
	
	private Map<String,Integer> vector = null;
	
	public VersionVector() {
		vector = new HashMap<String,Integer>();
	}

	public int count(String replica) {
		if (!vector.containsKey(replica)) {
			return 0;
		} else {
			return vector.get(replica);
		}
	}
	
	public Set<String> replicaSet() {
		return vector.keySet();
	}
	
	public void bump(String replica) {
		// Increment the replica's entry
		int count = this.count(replica);
		count++;
		vector.replace(replica, count);    	
	}
	
	public boolean dominates(VersionVector other) {
		// Check that each entry in other version vector is no greater than this one's
		boolean comparison = true;
		for (String replica : other.replicaSet()) {
			if (other.count(replica) > this.count(replica)) {
				comparison = false;
				break;
			}
		}
		return comparison;
	}
	
	public Map<String,Integer> toMap() {
		return vector;
	}
	
	public VersionVector fromMap(Map<String,Integer> map) {
		vector = map;
		return this;
	}

}
