package Firesniffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.util.List;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;

import com.datastax.driver.core.*;

public class Firesniff {

	public static void main(String[] args) throws Exception {

	        // Instantiate a Job object for your job's configuration.  

	        // Read recordset from Cassandra fs_stats table

	        Cluster cluster;
	        Session session;
	//        Iterable < Row > links = null;

			// Connect to Cassandra DB
	        cluster = Cluster.builder().addContactPoints("firesniffer01", "firesniffer02", "firesniffer03")
	            .withPort(9042)
	            .withCredentials("cassandra", "cassandra")
	            .build();
	        session = cluster.connect("fsdev");
			
			// Query fs_errorcode_config table to load error codes and error severity (info, warning, severe)
	        ResultSet results = session.execute("SELECT errorcode, severity FROM fs_errorcode_config WHERE rule_enable=true ALLOW FILTERING");

			// Assign results to a list
	        List < Row > resultError = results.all();
	        List<String> table_error = new ArrayList<String>();
	        List<String> table_severity = new ArrayList<String>();
	   //     List<String> severity = new ArrayList<String>();
            
	            for (Row row: resultError) {
	            table_error.add(row.getString("errorcode"));
	            table_severity.add(row.getString("severity"));    
	        }	
	        
	        List<String> error_info = new ArrayList<String>();
	        List<String> error_warn = new ArrayList<String>();
	        List<String> error_severe = new ArrayList<String>();
	        List<ArrayList<String>> error = new ArrayList<ArrayList<String>>();
	            
	        for (int z = 0; z<table_error.size();z++){
	        	if(table_severity.get(z).equals("info")){
	        		error_info.add(table_error.get(z));
	        	}
	        	if(table_severity.get(z).equals("warning")){
	        		error_warn.add(table_error.get(z));
	        	}
	        	if(table_severity.get(z).equals("severe")){
	        		error_severe.add(table_error.get(z));
	        	}
	        }
	        
	        error.add((ArrayList<String>)error_info);
	        error.add((ArrayList<String>)error_warn);
	        error.add((ArrayList<String>)error_severe);
	            
			// Query fs_stats table
	        ResultSet stats_id_result = session.execute("SELECT stats_id FROM fs_stats WHERE processed = false ALLOW FILTERING");
	        
			// Assign result to a list
	        List < Row > stats_id_results = stats_id_result.all();
	        for (Row rows: stats_id_results) {

	        	String Path = rows.getString("stats_id");

	        	System.out.println("Sniffer: begin to process stats_id = " + Path);

	            Configuration conf = new Configuration();
	            conf.set("stats_id", Path);	            
	            int counter = 0;
	            boolean success = true;
	            for (int x=0; x<3;x++) {
	            	
	            	if (counter==0){
	            		String[] err = error_info.toArray(new String[error_info.size()]);
	            		conf.setStrings("errors", err);
	            		System.out.println("Begin to process Info for stats_id = " + Path + "--------------------------");
	            	}
	            	if (counter==1){
	            		String[] err = error_warn.toArray(new String[error_warn.size()]);
	            		conf.setStrings("errors", err);
	            		System.out.println("Begin to process Warning for stats_id = " + Path + "--------------------------");
	            	}
	            	if (counter==2){
	            		String[] err = error_severe.toArray(new String[error_severe.size()]);
	            		conf.setStrings("errors", err);
	            		System.out.println("Begin to process Severe for stats_id = " + Path + "--------------------------");
	            	}
	           	
		            conf.set("counter", String.valueOf(counter));
		            counter++;
		            //String[] sev = severity.toArray(new String[severity.size()]);
		            //conf.setStrings("severe", sev);
		            
		            Job job = new Job(conf);
		            
		            // Specifies the jar file that contains your driver, mapper, and reducer.
		            job.setJarByClass(Firesniff.class);

					// Job name
		            job.setJobName("Firesniffer");

		            // Specifies input and output files
		            FileInputFormat.setInputPaths(job, new Path(Path));

		            // Delete output if exists
		            Path outputDir = new Path("/home/fsuser/fs_logs/Jian_PC/Hadoop/output");
		            FileSystem hdfs = FileSystem.get(conf);
		            if (hdfs.exists(outputDir))
		                hdfs.delete(outputDir, true);
		            FileOutputFormat.setOutputPath(job, new Path("/home/fsuser/fs_logs/Jian_PC/Hadoop/output"));

		            // Specifies the mapper and reducer classes.
		            job.setMapperClass(FiresnifferMapper.class);
		            job.setReducerClass(FiresnifferReducer.class);

		            // Specifies the job's output key and value classes.
		            job.setOutputKeyClass(Text.class);
		            job.setOutputValueClass(IntWritable.class);

		            job.setNumReduceTasks(1);

					// Start MapReduce and wait for completion
		            success = job.waitForCompletion(true);	
		        }

		        // Timestamp
	            Date now = new Date();
	            Timestamp nowTS = new Timestamp(now.getTime());

		        // Update fs_stats table if job completed successfully 
	            if (success) {
	                session.execute("UPDATE fs_stats SET processed=true, processed_date=? WHERE stats_id=?", nowTS, Path);
	                System.out.println("Finished process for stats_id = " + Path);
	            } else {
	                System.out.println("Job not completed successfully");
	                continue;
	            }

			}

	        System.exit(0);
    }
}
