package com.hbase.helper;

import java.io.IOException;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

public class DropTable {
	private static DropTable dropTable = null;
	private Connection conn = null;
	
	private DropTable() throws IOException {
		conn = Connector.getInstance().getConnection();
	}
	
	public static DropTable getInstance() throws IOException {
		if(dropTable == null) {
			dropTable = new DropTable();
		}
		
		return dropTable;
	}
	
	public void dropTable(String tableName) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		Admin admin = conn.getAdmin();
		
		try {
			if(admin.isTableAvailable(TableName.valueOf(tableName))) {
				//Disabling table name
				admin.disableTable(TableName.valueOf(tableName));
				
				//Deleting table name
				admin.deleteTable(TableName.valueOf(tableName));
				
				System.out.println("Table deleted");
			} else {
				System.out.println("Table does not exist");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			//Close Admin
			if(admin != null) {
				admin.close();
			}
		}
	}
}
