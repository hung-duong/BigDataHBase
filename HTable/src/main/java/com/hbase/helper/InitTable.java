package com.hbase.helper;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

public class InitTable {
	private static InitTable initTable = null;
	private Connection conn = null;
	
	private InitTable() throws IOException {
		conn = Connector.getInstance().getConnection();
	}
	
	public static InitTable getInstance() throws IOException {
		if(initTable == null) {
			initTable = new InitTable();
		}
		
		return initTable;
	}
	
	public void initTable(String tableName, String[] columnsFamily) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		//Instantiating HBaseAdmin class
		Admin admin = conn.getAdmin();
		
		if(admin.isTableAvailable(TableName.valueOf(tableName))) {
			System.out.println("Table existed");
		} else {
			//Instantiating table descriptor class
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
			
			//Add column families to table descriptor
			for(String cl : columnsFamily) {
				tableDescriptor.addFamily(new HColumnDescriptor(cl));
			}
			
			//Execute the table through admin
			admin.createTable(tableDescriptor);
			System.out.println("Table created");
		} 
	}
	
}
