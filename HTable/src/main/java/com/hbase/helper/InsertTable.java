package com.hbase.helper;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class InsertTable {
	private static InsertTable insertTable = null;
	private Connection conn = null;
	
	private InsertTable() throws IOException {
		conn = Connector.getInstance().getConnection();
	}
	
	public static InsertTable getInstance() throws IOException {
		if(insertTable == null) {
			insertTable = new InsertTable();
		}
		
		return insertTable;
	}
	
	public void createData(String tableName, String rowName, 
			String columnFamily, String columnQualifier, String value) throws IOException {
		
		//Instantiating Table class
		Table table = conn.getTable(TableName.valueOf(tableName));
		
		try {	
			//Instantiating put class
			Put put = new Put(Bytes.toBytes(rowName));
			
			//Insert data
			byte[] hcolumnFamily = Bytes.toBytes(columnFamily);
			byte[] hcolumnQualifier = Bytes.toBytes(columnQualifier);
			byte[] hvalue = Bytes.toBytes(value);
			put.addColumn(hcolumnFamily, hcolumnQualifier, hvalue);
			
			//Save the put Instance to the HTable
			table.put(put);
			System.out.println("Data inserted");
			
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			//Close Table
			if(table != null) {
				table.close();
			}
		}
	}
			
}
