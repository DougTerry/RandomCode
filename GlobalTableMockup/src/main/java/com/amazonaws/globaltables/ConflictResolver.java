package com.amazonaws.globaltables;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.document.Item;

public class ConflictResolver {

	public ConflictResolver() {
	}
	
	/*
	 * Returns true if the two items are conflicting versions
	 */	
	public boolean isConflict(Item one, Item two) {
		boolean conflicted = true;
		if (one == null || two == null) {
			return false;
		}
		VersionVector oneVersion = SystemAttributes.getVersion(one);
		VersionVector twoVersion = SystemAttributes.getVersion(two);
		if (oneVersion.dominates(twoVersion) || twoVersion.dominates(oneVersion)) {
			conflicted = false;
		}
		return conflicted;
	}
	
	/*
	 * Returns true if the first item should be chosen as the winner when conflicting with the second item
	 */	
	public boolean isWinner(Item one, Item two, Regions master) {
		boolean oneWins = false;;
		VersionVector oneVersion = SystemAttributes.getVersion(one);
		VersionVector twoVersion = SystemAttributes.getVersion(two);
		String oneOrigin = SystemAttributes.getOrigin(one);
		String twoOrigin = SystemAttributes.getOrigin(two);
		Long oneTimestamp = SystemAttributes.getTimestamp(one);
		Long twoTimestamp = SystemAttributes.getTimestamp(two);

		if (oneVersion.dominates(twoVersion)) {  // no conflict
			oneWins = true;
		} else if (twoVersion.dominates(oneVersion)) {  // no conflict
			oneWins = false;
		} else if (oneOrigin.equals(master.getName())) {  // master update wins
			oneWins = true;
		} else if (twoOrigin.equals(master.getName())) {  // master update wins
			oneWins = false;
		} else {
			oneWins = oneTimestamp > twoTimestamp;  // latest timestamp wins
		}
		return oneWins;
	}

}
