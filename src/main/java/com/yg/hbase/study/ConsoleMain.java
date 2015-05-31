package com.yg.hbase.study;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class ConsoleMain {
	
	private static void filterSample(HTablePool pool) {
		HTableInterface users = pool.getTable("twits");
		
		try {
			Scan scan = new Scan();
			scan.addColumn(Bytes.toBytes("twits"), Bytes.toBytes("twit"));
			
			Filter filter = new ValueFilter(CompareOp.EQUAL, new RegexStringComparator(".*responsibly.*"));
			scan.setFilter(filter);
			
			ResultScanner rs = users.getScanner(scan);
			Result result = null;
			int i = 0;
			while((result = rs.next()) != null) {
				System.out.print("Count Result : " + i++);
				System.out.println(" Row >" + new String(result.getRow()) +  " ::: " +
						new String(result.getValue("twits".getBytes(), "twit".getBytes())));
			}
			
			
			pool.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static void main(String ... v) {
		Configuration conf = HBaseConfiguration.create();
//		conf.setInt("timeout", 5000);
		conf.set("hbase.zookeeper.quorum", "192.168.25.37");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.set("hbase.master", "192.168.25.37:60000");
//		conf.set("hbase.zookeeper.property.clientPort", "12181");
//		conf.set("hbase.master.port", "41000");
//		conf.set("hbase.master.info.port", "41010");
		
		System.out.println(conf.get("hbase.zookeeper.property.clientPort"));
		System.out.println(conf.get("hbase.master.port"));
		System.out.println(conf.get("hbase.master.info.port"));
		
		
		HTablePool pool = new HTablePool(conf, Integer.MAX_VALUE);
		
		HTableInterface users = pool.getTable("twits");
		
		try {
			Scan scan = new Scan();
			scan.addColumn(Bytes.toBytes("twits"), Bytes.toBytes("twit"));
			
			Filter filter = new ValueFilter(CompareOp.EQUAL, new RegexStringComparator(".*responsibly.*"));
			scan.setFilter(filter);
			
			ResultScanner rs = users.getScanner(scan);
			Result result = null;
			int i = 0;
			while((result = rs.next()) != null) {
				System.out.print("Count Result : " + i++);
				System.out.println(" Row >" + new String(result.getRow()) +  " ::: " +
						new String(result.getValue("twits".getBytes(), "twit".getBytes())));
			}
			
			
			pool.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
