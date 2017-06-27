package demo.habse.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import demo.habse.util.Constants;
import demo.hbase.connection.HbaseConnection;
import demo.hbase.repo.HbaseRepo;
import demo.hbase.repo.HbaseRepoImpl;

/**
 * @author Pranav 
 * 26-Jun-2017
 *
 */
public class Main {
	
	public static void main(String[] args) throws IOException  {
		
		Connection conn =ConnectionFactory.createConnection(new Configuration());
		Admin admin = conn.getAdmin();
		HbaseConnection connectionConfig= new HbaseConnection();
		connectionConfig.setAdmin(admin);
		connectionConfig.setTableName(Constants.TABLE_NAME);
		HbaseRepo hbase= new HbaseRepoImpl();
		try {
			hbase.deleteTable(connectionConfig);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		try {
			hbase.createTable(connectionConfig);
			hbase.addDataIntoTable(connectionConfig);
			hbase.getByFirstName(connectionConfig, Constants.DATA_FIRST_NAME);
			hbase.fullTextSearch(connectionConfig, Constants.DATA_REGX);
			hbase.deleteByRowId(connectionConfig, Constants.DATA_ROW_ID_TO_DELELE);
			hbase.fullTextSearch(connectionConfig, Constants.DATA_REGX);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		conn.close();
	}
}

