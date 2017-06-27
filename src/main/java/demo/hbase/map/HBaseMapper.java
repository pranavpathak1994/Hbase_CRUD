package demo.hbase.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Pranav 
 * 26-Jun-2017
 *
 */
public class HBaseMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
	
	final static byte[] COL_FAMILY = "bookFamily".getBytes();
	 
	List<String> columnList = new ArrayList<String>();
	
	ImmutableBytesWritable hKey = new ImmutableBytesWritable();
	KeyValue kv;
	
	/* 
	 * map to insert data 
	 */
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.map(key, value, context);
	}
}
