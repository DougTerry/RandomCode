package com.amazonaws.globaltables;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.amazonaws.regions.Regions;

public class VersionVector {

	/*
	 * A version vector is a mapping from replica names to counts of the number of updates made by each replica.
	 * A missing entry implies a count of zero.
	 */
	
	private Map<String,Integer> vector = null;
	
	public VersionVector() {
		vector = new HashMap<String,Integer>();
	}

	public VersionVector(Regions replica) {
		vector = new HashMap<String,Integer>();
		vector.put(replica.getName(), 1);
	}

	public int count(Regions replica) {
		if (!vector.containsKey(replica.getName())) {
			return 0;
		} else {
			return vector.get(replica.getName());
		}
	}
	
	private int count(String replicaName) {
		if (!vector.containsKey(replicaName)) {
			return 0;
		} else {
			return vector.get(replicaName);
		}
	}
	
	public Set<Regions> replicaSet() {
		Set<Regions> regions = new HashSet<Regions>();
		for (String regionName : vector.keySet()) {
			regions.add(Regions.fromName(regionName));
		}
		return regions;
	}
	
	public VersionVector bump(Regions replica) {
		// Increment the replica's entry
		String regionName = replica.getName();
		if (!vector.containsKey(regionName)) {
			vector.put(regionName, 1);
		} else {
			int count = this.count(regionName);
			count++;
			vector.replace(regionName, count);    	
		}
		return this;
	}
	
	public boolean dominates(VersionVector other) {
		// Check that each entry in other version vector is no greater than this one's
		boolean comparison = true;
		for (String replicaName : other.vector.keySet()) {
			if (other.count(replicaName) > this.count(replicaName)) {
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
