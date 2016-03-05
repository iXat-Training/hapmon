package com.hapmon.mapreduce.aspects;

public class JobDto {

	private String jobName;
	
	private String jobId;
	
	private long totalExecutionTime;
	
	private long mapperExecutionTime;
	
	private long reducerExecutionTime;
	
	private String mapperName;
	
	private String reducerName;
	
	private String hostName;

	
	
	
	public String getMapperName() {
		return mapperName;
	}

	public void setMapperName(String mapperName) {
		this.mapperName = mapperName;
	}

	public String getReducerName() {
		return reducerName;
	}

	public void setReducerName(String reducerName) {
		this.reducerName = reducerName;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public long getTotalExecutionTime() {
		return totalExecutionTime;
	}

	public void setTotalExecutionTime(long totalExecutionTime) {
		this.totalExecutionTime = totalExecutionTime;
	}

	public long getMapperExecutionTime() {
		return mapperExecutionTime;
	}

	public void setMapperExecutionTime(long mapperExecutionTime) {
		this.mapperExecutionTime = mapperExecutionTime;
	}

	public long getReducerExecutionTime() {
		return reducerExecutionTime;
	}

	public void setReducerExecutionTime(long reducerExecutionTime) {
		this.reducerExecutionTime = reducerExecutionTime;
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	
	
}
