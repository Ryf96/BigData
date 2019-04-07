import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Arrays;
public class Hw1Grp4 {


	public static void HBaseWrite(int row, String column, int[] column_index) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		Logger.getRootLogger().setLevel(Level.WARN);

		String tableName = "Result";
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));

	    // create column descriptor
   	    HColumnDescriptor cf = new HColumnDescriptor("res");
	    htd.addFamily(cf);
	
	    // configure HBase
	    Configuration configuration = HBaseConfiguration.create();
	    HBaseAdmin hAdmin = new HBaseAdmin(configuration);
	
	    if (hAdmin.tableExists(tableName)) {
	        System.out.println("Table already exists");
	    }
	    else {
	        hAdmin.createTable(htd);
	        System.out.println("table "+tableName+ " created successfully");
	    }
	    hAdmin.close();


	    HTable table = new HTable(configuration,tableName);
	    String row_s = "" + row;
      	Put put = new Put(row_s.getBytes());
      	String[] column_val = column.split(",");
      	for (int i = 0; i < column_val.length; i++) {
      		put.add("res".getBytes(),("R" + column_index[i]).getBytes(),column_val[i].getBytes());
      	}

    	table.put(put);
      	table.close();
      	System.out.println("put successfully");
	}

	
	public static void main(String[] args) throws IOException, URISyntaxException{
		if (args.length != 3) {
			System.out.println("Usage: Hw1Grp4 R=<file> select:<cloumn>,<op>,<num> distinct:<column>");
			System.exit(1);
		}
		String file = args[0].substring(2);
		System.out.println("BreakPoint 1");
        
		Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(file), conf);
		System.out.println("HDFS breakpoint");
	    Path path = new Path(file);
	    FSDataInputStream in_stream = fs.open(path);
	
	    BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
		String s;
		// Parse the select condition
		String[] select_condition = (args[1].substring(7)).split(",");
		Pattern p = Pattern.compile("(\\d+)");
		Matcher m1 = p.matcher(select_condition[0]);
		int select_column = 0;
		if(m1.find()) {
			select_column = Integer.parseInt(m1.group(1));
		}
		else {
			System.out.println("Select condition wrong!");
			System.exit(1);
		}

		//exec the selecting filter
		ArrayList<String[]> values = new ArrayList<String[]>();
		values.clear();
		while((s=in.readLine())!=null) {
			String[] tmp_str = s.split("\\|");
			System.out.println("line: " + s);
			System.out.println("op: " + select_condition[1]);
					switch(select_condition[1]) {
				case "gt" :
					if (Long.parseLong(tmp_str[select_column]) > Long.parseLong(select_condition[2])) {
						values.add(tmp_str);
					}
					break;
				case "ge" :
					if (Long.parseLong(tmp_str[select_column]) >= Long.parseLong(select_condition[2])) {
						values.add(tmp_str);
					}
					break;
				case "eq" :
					if (Long.parseLong(tmp_str[select_column]) == Long.parseLong(select_condition[2])) {
						values.add(tmp_str);
					}
					break;
				case "ne" :
					if (Long.parseLong(tmp_str[select_column]) != Long.parseLong(select_condition[2])) {
						values.add(tmp_str);
					}
					break;
				case "le" :
					if (Long.parseLong(tmp_str[select_column]) <= Long.parseLong(select_condition[2])) {
						values.add(tmp_str);
					}
					break;
				case "lt" :
					if (Long.parseLong(tmp_str[select_column]) < Long.parseLong(select_condition[2])) {
						values.add(tmp_str);
					}
					break;
				default:
					System.out.println("options input error!");
					System.exit(1);
					break;	
			}
		}
	    in.close();
		fs.close();
	    //parse the distinct column
	    String[] dis_condition = (args[2].substring(9)).split(",");
	    int[] dis_column = new int[dis_condition.length];
	    Matcher m2;
		String regEx = "[^0-9]";
		Pattern p2 = Pattern.compile(regEx);
	    for (int i = 0; i < dis_condition.length; i++) {
			System.out.println("dis_condition: "+dis_condition[i]);
	    	m2 = p2.matcher(dis_condition[i]);
	    	dis_column[i] = Integer.parseInt(m2.replaceAll("").trim());
			System.out.println("dis_column[i]: " + dis_column[i]);
	    }

	    //construct a hash map
	    String dis_value;
	    HashMap mymap = new HashMap();
		String[][] values_ss = new String[values.size()][];
		values.toArray(values_ss);
	        
		for (int j = 0; j < values.size(); j++) {
	    	dis_value = "";
	    	for(int k = 0; k < dis_column.length; k++) {
	    		dis_value += (String) values_ss[j][dis_column[k]] + ",";

	    	}
	    	mymap.put(dis_value, null);
			System.out.println("dis_value: "+dis_value);
	    }

	    //flush into HBase
	    String key;
	    int l = 0;
	    Iterator iter = mymap.keySet().iterator();
	    while (iter.hasNext()) {
	    	key = (String)iter.next();
	    	HBaseWrite(l, key, dis_column);
	    	l++;
	    }
	}
}





