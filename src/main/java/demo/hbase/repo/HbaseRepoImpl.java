package demo.hbase.repo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import demo.habse.util.Constants;
import demo.hbase.connection.HbaseConnection;

/**
 * @author Pranav 
 * 26-Jun-2017
 *
 */
public class HbaseRepoImpl implements HbaseRepo{
	
	 private final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
	 
	 /*
	  * Create a random String
	  */
	 private String getRandomString(){
		 
	        StringBuilder salt = new StringBuilder();
	        Random rnd = new Random();
	        while (salt.length() < 18) { // length of the random string.
	            int index = (int) (rnd.nextFloat() * ALPHABET.length());
	            salt.append(ALPHABET.charAt(index));
	        }
	        String saltStr = salt.toString();
	        return saltStr;
	 }
	
	/*
	 * Validate the connection
	 */
	private void isValidConnection(HbaseConnection hbaseConnection){
		if(hbaseConnection ==null || hbaseConnection.getTableName()==null || hbaseConnection.getAdmin()==null)
			throw new NullPointerException(Constants.EXCEPTION_INCOMPLETE_CONFIG);
	}
	/*
	 * Create table if not exist
	 */
	@Override
	public void createTable(HbaseConnection hbaseConnection) throws IOException{
		
		isValidConnection(hbaseConnection);
		
		TableName table= TableName.valueOf(hbaseConnection.getTableName());
		
		if(!hbaseConnection.getAdmin().tableExists(table)){	
			HTableDescriptor htable = new HTableDescriptor(table);
			htable.addFamily(new HColumnDescriptor(Constants.ADDRESS_COLUMN));
			htable.addFamily(new HColumnDescriptor(Constants.NAME_COLUMN));
			hbaseConnection.getAdmin().createTable(htable);
			System.out.println(String.format(Constants.SUCCESS_TABLE_CREATED,hbaseConnection.getTableName()));
		}else
			throw new TableExistsException(String.format(Constants.EXCEPTION_TABLE_ALREADY_EXIST, hbaseConnection.getTableName()));
	}

	/*
	 * delete exist table
	 */
	@Override
	public void deleteTable(HbaseConnection hbaseConnection) throws IOException {
		TableName table=TableName.valueOf(hbaseConnection.getTableName());
		if(hbaseConnection.getAdmin().tableExists(table)){
			hbaseConnection.getAdmin().disableTable(table);
			hbaseConnection.getAdmin().deleteTable(table);
			System.out.println(String.format(Constants.SUCCESS_TABLE_DELETED,hbaseConnection.getTableName()));
		}else
			 throw new TableNotFoundException(String.format(Constants.EXCEPTION_TABLE_NOT_FOUND,hbaseConnection.getTableName()));
		
		
	}
	
	/*
	 *insert random data into table  
	 */
	@Override
	public void addDataIntoTable(HbaseConnection hbaseConnection) throws IOException {
		Table table= hbaseConnection.getAdmin().getConnection().getTable(TableName.valueOf(hbaseConnection.getTableName()));
		List<Put> puts= new ArrayList<>();
		for(int i=0;i<10000;i++){
			Put p = new Put(Bytes.toBytes(Constants.ROW_ID+i));
			if(i==7400){
				p.addColumn(Bytes.toBytes(Constants.NAME_COLUMN),Bytes.toBytes(Constants.FIRST_NAME),Bytes.toBytes(Constants.DATA_FIRST_NAME));
				p.addColumn(Bytes.toBytes(Constants.NAME_COLUMN),Bytes.toBytes(Constants.LAST_NAME),Bytes.toBytes(Constants.DATA_LAST_NAME));
				p.addColumn(Bytes.toBytes(Constants.ADDRESS_COLUMN),Bytes.toBytes(Constants.CITY),Bytes.toBytes(Constants.DATA_CITY));
				p.addColumn(Bytes.toBytes(Constants.ADDRESS_COLUMN),Bytes.toBytes(Constants.STATE),Bytes.toBytes(Constants.DATA_STATE));
				p.addColumn(Bytes.toBytes(Constants.ADDRESS_COLUMN),Bytes.toBytes(Constants.COUNTRY),Bytes.toBytes(Constants.DATA_COUNTRY));
			}else{
				p.addColumn(Bytes.toBytes(Constants.NAME_COLUMN),Bytes.toBytes(Constants.FIRST_NAME),Bytes.toBytes(getRandomString()));
				p.addColumn(Bytes.toBytes(Constants.NAME_COLUMN),Bytes.toBytes(Constants.LAST_NAME),Bytes.toBytes(getRandomString()));
				p.addColumn(Bytes.toBytes(Constants.ADDRESS_COLUMN),Bytes.toBytes(Constants.CITY),Bytes.toBytes(getRandomString()));
				p.addColumn(Bytes.toBytes(Constants.ADDRESS_COLUMN),Bytes.toBytes(Constants.STATE),Bytes.toBytes(getRandomString()));
				p.addColumn(Bytes.toBytes(Constants.ADDRESS_COLUMN),Bytes.toBytes(Constants.COUNTRY),Bytes.toBytes(getRandomString()));
			}
			puts.add(p);
		}
		System.out.println(String.format(Constants.SUCCESS_DATA_INSERTED, hbaseConnection.getTableName()));
		table.put(puts);
		table.close();
	}

	/* 
	 * Get record by firstName
	 */
	@Override
	public void getByFirstName(HbaseConnection hbaseConnection, String firstName) throws IOException {
		
		Scan scan= new Scan();		
		Table table=hbaseConnection.getAdmin().getConnection().getTable(TableName.valueOf(Constants.TABLE_NAME));

		SingleColumnValueFilter filterByName = new SingleColumnValueFilter( 
                Bytes.toBytes(Constants.NAME_COLUMN ),
                Bytes.toBytes(Constants.FIRST_NAME),
                CompareOp.EQUAL,
                Bytes.toBytes(firstName));
		scan.setFilter(filterByName);
		
		ResultScanner results =table.getScanner(scan);
		
		try {
			System.out.println("***************** First Name Search *****************");
			for (Result result : results){
				
				for (Cell cell: result.listCells()) {
					String row = new String(CellUtil.cloneRow(cell));
				    String family = new String(CellUtil.cloneFamily(cell));
				    String column = new String(CellUtil.cloneQualifier(cell));
				    String value = new String(CellUtil.cloneValue(cell));
				    long timestamp = cell.getTimestamp();
				    System.out.printf("%-20s column=%s:%s, timestamp=%s,value=%s\n", row, family, column, timestamp, value);
				}
				
			}
			System.out.println("***************** End First Name Search *****************");
		} catch (Exception e) {
			
		}finally {
			results.close();
			table.close();
		}		
	}

	/* 
	 * Full text search
	 */
	@Override
	public void fullTextSearch(HbaseConnection hbaseConnection, String keyword) throws IOException {
		
		Scan scan= new Scan();		
		Table table=hbaseConnection.getAdmin().getConnection().getTable(TableName.valueOf(Constants.TABLE_NAME));
		
		ValueFilter valueFilter= new ValueFilter(CompareOp.EQUAL, new RegexStringComparator(keyword));
		scan.setFilter(valueFilter);
		
		ResultScanner results =table.getScanner(scan);
	
		try {
			long count=0;
			System.out.println(String.format("***************** Full Text Search by %s *****************", keyword));
			
			
			for (Result result : results){
				++count;
				result.listCells().stream().forEach(cell -> {
					String row = new String(CellUtil.cloneRow(cell));
				    String family = new String(CellUtil.cloneFamily(cell));
				    String column = new String(CellUtil.cloneQualifier(cell));
				    String value = new String(CellUtil.cloneValue(cell));
				    long timestamp = cell.getTimestamp();
				    System.out.printf("%-20s column=%s:%s, timestamp=%s,value=%s\n", row, family, column, timestamp, value);
				});
			}
			System.out.println(String.format("Total %d result found.", count));
			System.out.println("***************** End Full Text Search *****************");
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			results.close();
			table.close();
		}		
	}

	/* 
	 * Delete by RowId
	 */
	@Override
	public void deleteByRowId(HbaseConnection hbaseConnection, String rowId) throws IOException {
		
		Table table=hbaseConnection.getAdmin().getConnection().getTable(TableName.valueOf(Constants.TABLE_NAME));
		
		Delete d=new Delete(Bytes.toBytes(Constants.ROW_ID+rowId));
	    table.delete(d);
	    System.out.println(String.format(Constants.SUCCESS_ROW_DELETED, Constants.ROW_ID+rowId));
	    table.close();
	    
	}
	
	
	
}

