package com.hapmon.mapreduce.aspects;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapred.WrappedJvmID;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;

import com.influx.configuration.InfluxDB;
import com.influx.configuration.InfluxDBFactory;
import com.influx.configuration.Point;


public aspect Perf1 {

	private InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8112", "mr_user", "mr_password");
	private String dbName = "mr_metrics";

	pointcut jobDetails(Job job, Cluster cluster): 
		execution(* org.apache.hadoop.mapreduce.JobSubmitter.submitJobInternal(..))
		&& within(org.apache.hadoop.mapreduce.JobSubmitter)
		&& args(job,cluster);

	pointcut mapperExecTime(JobConf jobConf,TaskUmbilicalProtocol tup) : 
		execution(* org.apache.hadoop.mapred.MapTask.run(..))
		&& args(jobConf,tup);

	pointcut reducerExecTime(JobConf jobConf,TaskUmbilicalProtocol tup) : 
		execution(* org.apache.hadoop.mapred.ReduceTask.run(..))
		&& args(jobConf,tup);

	pointcut MRChildJVMInfo(InetSocketAddress taskAttemptListenerAddr, Task task, 
		      WrappedJvmID jvmID):
		 call(* org.apache.hadoop.mapred.MapReduceChildJVM.getVMCommand(..))
		 && args(taskAttemptListenerAddr,task,jvmID);
	
	
	/*pointcut afterJobCompletion(boolean flag) : 
		call(* org.apache.hadoop.mapreduce.Job.waitForCompletion(..))
		&& args(flag);*/

	JobStatus around(Job job,Cluster cluster) throws ClassNotFoundException, IOException, InterruptedException : jobDetails(job,cluster) {
		System.out.println("Mapper name: "+job.getMapperClass());
		System.out.println("Reducer name: "+job.getReducerClass());
		long start = System.currentTimeMillis();
		JobStatus js = proceed(job,cluster);
		long elapsedTime = System.currentTimeMillis() - start;
		System.out.println("Job Name : "+js.getJobName());
		System.out.println("Job ID : "+js.getJobID().getJtIdentifier());
		System.out.println("Job execution time: " + elapsedTime + " milliseconds.");
		

		Point point1 = Point.measurement("Job_Details")
		                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
		                    .field("job_id", job.getJobID().getJtIdentifier()).field("job_name", job.getJobName())
		                    .field("mapper_name", job.getMapperClass().getSimpleName())
		                    .field("reducer_name", job.getReducerClass().getSimpleName())
		                    .field("job_exec_time", elapsedTime)
		                    .build();

		influxDB.write(dbName, "default", point1);
		
		return js;
	}

	before(InetSocketAddress taskAttemptListenerAddr, Task task, 
		      WrappedJvmID jvmID): MRChildJVMInfo(taskAttemptListenerAddr,task,jvmID)
	{
		System.out.println("Before MRChildJVM......");
		System.out.println("JOOOBBB ID : "+task.getJobID().getJtIdentifier());
		System.out.println("host-name: "+taskAttemptListenerAddr.getHostName()+" port: "+taskAttemptListenerAddr.getPort());
		
		Point point1=null;
		if(jvmID.isMapJVM()){
		 point1 = Point.measurement("Mapper_Host_Details")
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .field("job_id", jvmID.getJobId().getJtIdentifier())
                .field("mapper_host_name", taskAttemptListenerAddr.getHostName())
                .build();
		}
		else{
			point1 = Point.measurement("Reducer_Host_Details")
	                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
	                .field("job_id", jvmID.getJobId().getJtIdentifier())
	                .field("reducer_host_name", taskAttemptListenerAddr.getHostName())
	                .build();
		}

		influxDB.write(dbName, "default", point1);
	}
	
	void around(JobConf jc,TaskUmbilicalProtocol tup) : mapperExecTime(jc,tup){
		long start = System.currentTimeMillis();
		proceed(jc,tup);
		long elapsedTime = System.currentTimeMillis() - start;
		System.out.println("JOB MAP: "+ jc.getJobName());
		System.out.println("Mapper execution time: " + elapsedTime + " milliseconds.");
		 InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8112", "mr_user", "mr_password");
		 String dbName = "mr_metrics";
		
		Point point1 = Point.measurement("Mapper_Metrics")
	                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
	                .field("job_name", jc.getJobName())
	                .field("mapper_name", jc.getMapperClass().getSimpleName())
	                .field("mapper_execution_time", elapsedTime)
	                .build();

		 influxDB.write(dbName, "default", point1);
	}

	void around(JobConf jc,TaskUmbilicalProtocol tup) : reducerExecTime(jc,tup){

		long start = System.currentTimeMillis();
		proceed(jc,tup);
		long elapsedTime = System.currentTimeMillis() - start;
		System.out.println("JOB RED: "+ jc.getJobName());
		System.out.println("Reducer execution time: " + elapsedTime + " milliseconds.");
		
		 InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8112", "mr_user", "mr_password");
		 String dbName = "mr_metrics";
		Point point1 = Point.measurement("Reducer_Metrics")
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .field("job_name", jc.getJobName())
                .field("mapper_name", jc.getReducerClass().getSimpleName())
                .field("mapper_execution_time", elapsedTime)
                .build();


		influxDB.write(dbName, "default", point1);

	}
	

	
}
