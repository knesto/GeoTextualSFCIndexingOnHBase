package gr.unipi.geotextualindex.sfc;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.SmallHilbertCurve;

import gr.unipi.geotextualindex.sfc.GeoUtil;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

//import org.apache.hadoop.hbase.TableName;

//import org.apache.hadoop.hbase.client.Table;


//import org.apache.hadoop.hbase.util.Bytes;

public class HbaseImportDataSFC {
	protected String filepath;

	protected HbaseImportDataSFC(String filepath) {
		this.filepath=filepath;  
       
    }
	
	protected void insertrows() throws IOException {
		 
		 BufferedReader br = null;
	        String line = "";
	        String cvsSplitBy = "\\|";
	       
	        //List<Document> doclist= new ArrayList<Document>();

	        //set parameters for hilbert function
	        int bits = 5;
	        int dimensions = 2;
	        //creation of hilbert object
	        SmallHilbertCurve h = HilbertCurve.small().bits(bits).dimensions(dimensions);
	        long maxOrdinates = 1L << bits;
	        //Adding documents to collection
	        try {
	    
	            br = new BufferedReader(new FileReader(this.filepath));
	            int count=0;
		        //Logger.getRootLogger().setLevel(Level.WARN);
		        Configuration conf = HBaseConfiguration.create();
		        Connection con = ConnectionFactory.createConnection(conf);
		       
		        String table = "geospatialdataindex";
		        Table htable = con.getTable(TableName.valueOf(table));
		        List <Put> puts = new ArrayList<>();
		        String hilberttext= null;
	            while ((line = br.readLine()) != null) {
	            	
	            	String[] data = line.split(cvsSplitBy);
	            	String[] textdata = data[5].split(",");
	                
	            	
			        for (int i = 0; i <textdata.length; i++) {
			        	//Put p = new Put(Bytes.toBytes(count));
			        	hilberttext=String.valueOf(h.index(GeoUtil.scale2DPoint(Double.valueOf(data[4]), -180d,180d,Double.valueOf(data[3]), -90d,90d, maxOrdinates)))+textdata[i].trim();
			        	Put p = new Put(Bytes.toBytes(hilberttext+"/"+String.valueOf(count)));
			        	p.addColumn(Bytes.toBytes("data"), Bytes.toBytes("hilbertindex"), Bytes.toBytes(hilberttext));
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
	
	
	  
	   

