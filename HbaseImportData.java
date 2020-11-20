package gr.unipi.geotextualindex.sfc;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseImportData {
	protected String filepath;

	protected HbaseImportData(String filepath) {
		this.filepath=filepath;  
       
    }
	
	protected void insertrows() throws IOException {
		 
		 BufferedReader br = null;
	        String line = "";
	        String cvsSplitBy = "\\|";
	       

	        //Adding documents to collection
	        try {
	    
	            br = new BufferedReader(new FileReader(this.filepath));
	            int count=0;
		        //Logger.getRootLogger().setLevel(Level.WARN);
		        Configuration conf = HBaseConfiguration.create();
		        Connection con = ConnectionFactory.createConnection(conf);
		       
		        String table = "geospatialdata";
		        Table htable = con.getTable(TableName.valueOf(table));
		        List <Put> puts = new ArrayList<>();
	            while ((line = br.readLine()) != null) {
	            	
	            	String[] data = line.split(cvsSplitBy);
	            	String[] textdata = data[5].split(",");
	                
	            	
			        for (int i = 0; i <textdata.length; i++) {
			        	Put p = new Put(Bytes.toBytes(String.valueOf(count)));
			        	p.addColumn(Bytes.toBytes("data"), Bytes.toBytes("groupid"), Bytes.toBytes(count));
			        	p.addColumn(Bytes.toBytes("data"), Bytes.toBytes("lon"), Bytes.toBytes(Double.valueOf(data[4])));
			        	p.addColumn(Bytes.toBytes("data"), Bytes.toBytes("lat"), Bytes.toBytes(Double.valueOf(data[3])));
			        	p.addColumn(Bytes.toBytes("data"), Bytes.toBytes("keyword"), Bytes.toBytes(textdata[i].trim()));
			        	puts.add(p);
			        	//htable.put(p);
			        }
			        	
			        count++;
			        }
	            htable.put(puts);
			       
	            
	        } catch (FileNotFoundException e) {
	            e.printStackTrace();
	        } catch (IOException e) {
	            e.printStackTrace();
	        } finally {
	            if (br != null) {
	                try {
	                    br.close();
	                    
	                } catch (IOException e) {
	                    e.printStackTrace();
	                }
	            }
	        }
	       
	  
	          

	    }
	 
	 
	
		      

		    
}
	
	
	  
	   

