package com.maven.spark;
import java.io.Serializable;


public class Average implements Serializable  {
	
	  private Double sum;
	  private long count;
	  
	  public Average(Double sum, long count) {
		super();
		this.sum = sum;
		this.count = count;
	  }

	public Double getSum() {
		return sum;
	}

	public void setSum(Double sum) {
		this.sum = sum;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}
	  	  
	  
}

