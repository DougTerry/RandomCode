package com.amazonaws.globaltables;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.regions.Regions;

public class Lease {

	// default lease duration (milliseconds)
	private static final long DEFAULT_DURATION = 1000*60*5L;  // 5 minutes
	
	// bound on clock asynchrony between machines (milliseconds)
	private static final long CLOCK_BOUND = 1000*2L;  // 2 seconds
	
	// lag time to renew before expiration (milliseconds)
	private static final long TIME_TO_RENEW = 1000*60*1L;  // 1 minute
	
	// lease owner
	private Regions owner;
	
	// lease expiration
	private Date expiration;
	
	// next owner for transition lease
	private Regions nextOwner;
	
	// next expiration for transition lease
	private Date nextExpiration;

	/*
	 * Create new lease
	 */
	
	public Lease() {
		// new lease that already expired with no owner
		owner = null;
		expiration = new Date(0);
		nextOwner = null;
		nextExpiration = new Date(0);
	}
	
	public Lease(Regions owner) {
		// new lease for the given owner
		this.owner = owner;
		expiration = new Date(System.currentTimeMillis() + DEFAULT_DURATION);
		nextOwner = null;
		nextExpiration = new Date(0);
	}
	
	/*
	 * Get information about current lease
	 */
	
	public Regions getOwner() {
		return owner;
	}
	
	public Date getExpiration() {
		return expiration;
	}

	public Regions getNextOwner() {
		return nextOwner;
	}

	public Date getNextExpiration() {
		return nextExpiration;
	}
	
	public boolean isExpired() {
		boolean expired = System.currentTimeMillis() > expiration.getTime() + CLOCK_BOUND;
		return expired;
	}
	
	public boolean maybeExpired() {
		boolean expired = System.currentTimeMillis() > expiration.getTime() - CLOCK_BOUND;
		return expired;
	}
	
	public boolean almostExpired() {
		boolean almost = System.currentTimeMillis() > expiration.getTime() - TIME_TO_RENEW;
		return almost;
	}
	
	public boolean isNextExpired() {
		boolean expired = System.currentTimeMillis() > nextExpiration.getTime() + CLOCK_BOUND;
		return expired;
	}
	
	public boolean maybeNextExpired() {
		boolean expired = System.currentTimeMillis() > nextExpiration.getTime() - CLOCK_BOUND;
		return expired;
	}

	/*
	 * Main operations on leases
	 */
	
	/*
	 * Extend the lease if the current owner and no transition in progress
	 */
	public boolean renew(Regions owner) {
		if (nextOwner != null) {  // transition
			if (owner != nextOwner) {  // cannot renew lease if not the next owner
				return false;  
			} 
			else if (!this.isExpired()) {  // cannot renew the next lease if not yet active
				return false;
			}
			else {  // active transition lease being renewed by owner
				this.owner = owner;
				this.expiration.setTime(System.currentTimeMillis() + DEFAULT_DURATION);		
				this.nextOwner = null;
				this.nextExpiration.setTime(0);						
			}
		} 
		else if (owner == this.owner) {  // renew by extending expiration
			this.expiration.setTime(System.currentTimeMillis() + DEFAULT_DURATION);		
		}
		else {  // cannot renew lease if not the current owner
			return false;
		}
		return true;
	}

	/*
	 * Release the lease if the current owner
	 */
	public boolean release(Regions owner) {
		if (owner != this.owner) {  // can only release a lease that you own
			return false;
		}
		this.owner = null;
		this.expiration.setTime(0);
		return true;
	}
	
	/*
	 * Acquire the lease if nobody else already has it
	 */
	public boolean acquire(Regions owner) {
		if (!this.isExpired() ) {  // current lease still good
			return false;
		}
		else if (nextOwner != null && !this.isNextExpired()) {  // transition in progress
			return false;
		}
		this.owner = owner;
		this.expiration.setTime(System.currentTimeMillis() + DEFAULT_DURATION);		
		this.nextOwner = null;
		this.nextExpiration.setTime(0);		
		return true;
	}
	
	/*
	 * Forcefully take over the lease when it expires
	 */
	public boolean take(Regions owner) {
		if (!this.isExpired() ) {  // delay transition until after current lease
			nextOwner = owner;
			nextExpiration.setTime(expiration.getTime() + DEFAULT_DURATION);	
			return false;
		}
		else {  // take ownership immediately
			this.owner = owner;
			this.expiration.setTime(System.currentTimeMillis() + DEFAULT_DURATION);		
			nextOwner = null;
			nextExpiration.setTime(0);		
		}
		return true;
	}

}
