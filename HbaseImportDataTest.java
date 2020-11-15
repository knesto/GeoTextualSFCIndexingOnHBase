package gr.unipi.geotextualindex.sfc;

import org.junit.Test;

public class HbaseImportDataTest {
	
	 @Test
	 public void main() throws Exception {
		 
		 String file="/home/user/hbase/datasets/restaurants.txt";

		 HbaseImportData a= new HbaseImportData(file);
		 a.insertrows();
	 }
	 
	    public static void main(String[] args) throws Exception {

	    	org.junit.runner.JUnitCore.main("gr.unipi.geotextualindex.sfc.HbaseImportDataTest");            
	 }

}
