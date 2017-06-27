package demo.hbase.connection;

import org.apache.hadoop.hbase.client.Admin;

/**
 * @author Pranav 
 * 26-Jun-2017
 *
 */
public class HbaseConnection {
	
	private String tableName;
	
	private Admin admin;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public Admin getAdmin() {
		return admin;
	}

	public void setAdmin(Admin admin) {
		this.admin = admin;
	}
	
	

}
