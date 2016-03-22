package Firesniffer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Date;
import java.util.UUID;

import com.google.common.reflect.TypeToken;

import static com.google.common.collect.Lists.newArrayList;


public class FiresnifferReducer extends Reducer < Text, IntWritable, Text, IntWritable > {

	    Cluster cluster;
	    Session session;
	    PreparedStatement statement;
	    BoundStatement boundStatement;

	    String stats_id;
	    int counter;

	    public void setup(Context context) {
	        cluster = Cluster.builder().addContactPoints("firesniffer01", "firesniffer02", "firesniffer03")
	            .withPort(9042)
	            .withCredentials("cassandra", "cassandra")
	            .build();
	        session = cluster.connect("fsdev");

	        statement = session.prepare("INSERT INTO fs_stat_errors (id, error_code, error_count, error_description, level_of_severity, stats_id) VALUES (?,?,?,?,?,?)");
	        boundStatement = new BoundStatement(statement);

	        Configuration conf = context.getConfiguration();
	        stats_id = conf.get("stats_id");
	        counter = Integer.valueOf(conf.get("counter"));
	    }
	    
	    /*
	    The reduce method runs once for each key received from the shuffle and sort phase of the MapReduce framework.
	    The method receives a key of type Text, a set of values of type IntWritable, and a Context object.
	    */
	    
	    int errorCount = 0;
	    @Override
	    public void reduce(Text key, Iterable < IntWritable > values, Context context)
	    throws IOException,
	    InterruptedException {
	        int intEventCount = 0;

	        // For each value in the set of values passed to us by the mapper:   
	        for (IntWritable value: values) {

	            // Add the value to the SysLogEvent count counter for this key.
	            intEventCount += value.get();
	       		}

	        // Call the write method on the Context object to emit a key and a value from the reduce method.      
	        context.write(key, new IntWritable(intEventCount));
	        String[] splitkeys = key.toString().split("::");
	        Date date = new Date();
	        
	        if (key.getLength() > 0) {
	            String uuid = UUID.randomUUID().toString();
	            
	            if (counter==0) {
	            	session.execute(boundStatement.bind(uuid, splitkeys[0], intEventCount, splitkeys[1], "Info", stats_id));
	            } else if (counter==1) {
	            	session.execute(boundStatement.bind(uuid, splitkeys[0], intEventCount, splitkeys[1], "Warning", stats_id));
	            } else if (counter==2) {
	            	session.execute(boundStatement.bind(uuid, splitkeys[0], intEventCount, splitkeys[1], "Severe", stats_id));
	            }
	            System.out.println("add fs_stat_errors id = " + uuid + ", stats_id = " + stats_id + ", error_code = " + splitkeys[0]);
	            errorCount += intEventCount;
	        }
	        
	        if(counter==0){
	        	session.execute("UPDATE fs_stats SET info=? WHERE stats_id=?", errorCount, stats_id);
	        }
	        
	        if(counter==1){
	        	session.execute("UPDATE fs_stats SET warning=? WHERE stats_id=?", errorCount, stats_id);
	        }
	        
	        if(counter==2){
	        	session.execute("UPDATE fs_stats SET severe=? WHERE stats_id=?", errorCount, stats_id);
	        }
	    }

	    public void cleanup(Context context) {
	        cluster.close();
	    }
	}
