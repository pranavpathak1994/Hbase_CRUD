package demo.hbase.repo;

import java.io.IOException;

import demo.hbase.connection.HbaseConnection;

/**
 * @author Pranav 
 * 26-Jun-2017
 *
 */
public interface HbaseRepo {
	
	public void createTable(HbaseConnection hbaseConnection) throws IOException;
	
	public void deleteTable(HbaseConnection hbaseConnection) throws IOException;
	
	public void addDataIntoTable(HbaseConnection hbaseConnection) throws IOException;
	
	public void getByFirstName(HbaseConnection hbaseConnection, String firstName) throws IOException;
	
	public void fullTextSearch(HbaseConnection hbaseConnection, String keyword) throws IOException;
	
	public void deleteByRowId(HbaseConnection hbaseConnection, String rowId) throws IOException;
	
	
}
