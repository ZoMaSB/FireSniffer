package Firesniffer;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.Cluster;
//import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.reflect.TypeToken;

import static com.google.common.collect.Lists.newArrayList;

public class FiresnifferMapper extends Mapper < LongWritable, Text, Text, IntWritable > {
        	
	Cluster cluster;
    Session session;

    	List<String> error = new ArrayList<String>();

    	public void setup(Context context) {
			  cluster = Cluster.builder().addContactPoints("firesniffer01", "firesniffer02", "firesniffer03")
			            .withPort(9042)
			            .withCredentials("cassandra", "cassandra")
			            .build();
			        session = cluster.connect("fsdev");

		Configuration conf = context.getConfiguration();
		error = Arrays.asList(conf.getStrings("errors"));
    	}
	   
    	@Override
	    // The map method runs once for each line of text in the input file. The method receives a key of type LongWritable, a value of type Text, and a Context object.      
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	
			// Regex on which our mapping is based
		    String strLogEntryPattern = "\\b(" + StringUtils.join(error, "|") +"):|\\s+(?=(.{0,300}))";
		    Text strEvent = new Text("");

	    	String strLogEntryLine = value.toString();
	        Pattern objPtrn = Pattern.compile(strLogEntryPattern);
	        
	        Matcher objPatternMatcher = objPtrn.matcher(strLogEntryLine); 

	        while (!objPatternMatcher.find()){
	        	return;
	        }
	        
	        strEvent.set(objPatternMatcher.group(1) + "::" + objPatternMatcher.group(2));

	        // Emit key and value from map
	        context.write(strEvent, new IntWritable(1));
	    }
	   
	}
