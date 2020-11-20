package gr.unipi.geotextualindex.sfc;

import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.SmallHilbertCurve;
import org.junit.Test;



import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HilbertQuery6 {
	
	@Test
	 public void test1() throws Exception {
		
        int bits = 5;
        int dimensions = 2;
        SmallHilbertCurve h = HilbertCurve.small().bits(bits).dimensions(dimensions);
        long max = 1L << bits;
        double lon1= -82.937563; 
        double lat1= 40.19198813;
        double lon2=-50.6995;
        double lat2= 20.974964;
        double maxlon=180d;
        double minlon=-180d;
        double maxlat=90d;
        double minlat=-90d;
        String[] keywords = {"Burgers","Pizza","Sandwiches"};
 
        
        // Instantiating Configuration class
        Configuration config = HBaseConfiguration.create();
        //config.set("hbase.zookeeper.quorum", "localhost");
        //config.set("zookeeper.znode.parent", "/hbase-unsecure");
        //config.set("hbase.master", "localhost:60000");
        Connection connection = ConnectionFactory.createConnection(config);
        HilbertQuery q= new HilbertQuery(connection,"geospatialdataindex","data");
        List<String> sb = new ArrayList<String>();
        sb=q.getfilter(lon1,lon2, minlon, maxlon,  lat1,lat2, minlat, maxlat, keywords , max, h);
        q.exechbase(sb,lon1,lon2,lon2,lat2);
           
	}
	
    public static void main(String[] args) throws Exception {

    	org.junit.runner.JUnitCore.main("gr.unipi.geotextualindex.sfc.HilbertQuery6");            
 }

}
