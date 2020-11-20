package gr.unipi.geotextualindex.sfc;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;
import org.davidmoten.hilbert.Ranges;
import org.davidmoten.hilbert.SmallHilbertCurve;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
//import org.apache.hadoop.hbase.util.Bytes;
//import com.sun.tools.javac.util.List;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.HTable;
//import org.apache.hadoop.hbase.filter.Filter;
//import org.apache.hadoop.hbase.filter.FilterList;
//import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
//import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
//import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
//import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
//import org.apache.hadoop.hbase.filter.PrefixFilter;
//import org.apache.hadoop.hbase.filter.RegexStringComparator;


public class HilbertQuery {
	protected Connection con;
	protected String tname;
	protected String cname;
	
	
	
	public HilbertQuery(Connection con, String tname,String cname) {
		this.con=con;
		this.tname=tname;
		this.cname=cname;
	};
	
	public  void exechbase(List<String> hilbertresults, double lon1,double lon2,double lat1,double lat2) throws IOException {
		
        // Instantiating the Scan class
		//List<RowRange> ranges = new ArrayList<>();
        //allFilters.addFilter(new PrefixFilter(Bytes.toBytes("300Buffet")));
        //ranges.add(new RowRange(Bytes.toBytes("300American"), true, Bytes.toBytes("300American"), true));
        //scan.setFilter(new MultiRowRangeFilter(ranges));
        // filterList.filterRowKey(KeyValueUtil.createFirstOnRow(r2));
        // filterList.addFilter(new FuzzyRowFilter(list));
        // Scanning the required columns
        //scan.setFilter(filterList);
        //scan.addColumn(Bytes.toBytes(cname), Bytes.toBytes("lon"));
        //scan.addColumn(Bytes.toBytes(cname), Bytes.toBytes("lat"));
		Scan scan = new Scan();
        Table table = con.getTable(TableName.valueOf(tname));
        FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		//String[] hilbertresultsindex = hilbertresults.split(",");
		
		for(int i =0;i<(hilbertresults.size());i++) {
        allFilters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes("hilbertindex"), CompareOperator.EQUAL,Bytes.toBytes(hilbertresults.get(i))));
        }
        allFilters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes("lon"), CompareOperator.GREATER_OR_EQUAL,Bytes.toBytes(lon1)));
        allFilters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes("lon"), CompareOperator.LESS_OR_EQUAL,Bytes.toBytes(lon2)));
        allFilters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes("lat"), CompareOperator.LESS_OR_EQUAL,Bytes.toBytes(lat1)));
        allFilters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes("lat"), CompareOperator.GREATER_OR_EQUAL,Bytes.toBytes(lon2)));
        //allFilters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes("groupid"), CompareOperator.LESS_OR_EQUAL,Bytes.toBytes(30000)));

		scan.setFilter(allFilters);
        //scan.addFamily(Bytes.toBytes("data"));
        
        ResultScanner scanner = table.getScanner(scan);
        int c=0;
        long t1 = System.currentTimeMillis();
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
        //System.out.println("Found row : " + result);
        c++;
        }
        System.out.println("Number of rows : " + c);
        System.out.println("Calculation Time: " + (System.currentTimeMillis() - t1));
        scanner.close();
        table.close();
        con.close();
		
	}

    public  List<String> getfilter(double lon1,double lon2, double minLon, double maxLon, double lat1,double lat2, double minLat, double maxLat, String keywords[],long max,SmallHilbertCurve hc)throws ParseException {
        Ranges rangesList = hc.query(GeoUtil.scale2DPoint(lon1, minLon,maxLon,lat1, minLat,maxLat, max),
        		GeoUtil.scale2DPoint(lon2, minLon,maxLon,lat2, minLat,maxLat, max));
        //StringBuilder sb = new StringBuilder();
        List<String> sb = new ArrayList<String>();
        //StringBuilder sbkewords = new StringBuilder();
        //System.out.print(" ranges found: "+rangesList.size()  + " ");
        //retrieve hilbert values and concatenation of keywords 
      for(int word =0;word<keywords.length;word++){
    	  String keyword=keywords[word];
    	  //sbkewords.append("\""+keyword+"\""+",");
	        rangesList.stream().forEach(i->{
	                for(long k=i.low(); k<= i.high(); k++){
	                	sb.add(k+keyword);
	                    //sb.append("\""+k+keyword+"\""+",");                    
	                }
	            
	        });
      }   
        
        //sb.deleteCharAt(sb.length()-1);
        return sb;
    }
}
