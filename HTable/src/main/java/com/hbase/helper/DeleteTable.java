package com.hbase.helper;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class DeleteTable {
	private static DeleteTable deleteRowTable = null;
	private Connection conn = null;
	
	private DeleteTable() throws IOException {
		conn = Connector.getInstance().getConnection();
	}
	
	public static DeleteTable getInstance() throws IOException {
		if(deleteRowTable == null) {
			deleteRowTable = new DeleteTable();
		}
		
		return deleteRowTable;
	}
	
	public void deleteRow(String tableName, String rowName) throws IOException {
		//Instantiating Table class
		Table table = conn.getTable(TableName.valueOf(tableName));
		
		try {	
			//Instantiating Delete class
			Delete delete = new Delete(Bytes.toBytes(rowName));
			
			//Delete Data
			table.delete(delete);
			System.out.println("Row " + rowName + " is deleted");
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			//Close Table
			if(table != null) {
				table.close();
			}
		} 
	}
	
	public void deleteCell(String tableName, String rowName,
			String columnFamily, String columnQualifier) throws IOException {
		
		//Instantiating Table class
		Table table = conn.getTable(TableName.valueOf(tableName));
		
		try {	
			//Instantiating Delete class
			Delete delete = new Delete(Bytes.toBytes(rowName));
			
			byte[] hcolumnFamily = Bytes.toBytes(columnFamily);
			byte[] hcolumnQualifier = Bytes.toBytes(columnQualifier);
			delete.addColumn(hcolumnFamily, hcolumnQualifier);
			
			//Delete Data
			table.delete(delete);
			System.out.println("Cell is deleted");
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			//Close Table
			if(table != null) {
				table.close();
			}
		} 
	}
	
	public void deleteFamily(String tableName, String rowName, String columnFamily) throws IOException {
		
		//Instantiating Table class
		Table table = conn.getTable(TableName.valueOf(tableName));
		
		try {	
			//Instantiating Delete class
			Delete delete = new Delete(Bytes.toBytes(rowName));
			
			byte[] hcolumnFamily = Bytes.toBytes(columnFamily);
			delete.addFamily(hcolumnFamily);
			
			//Delete Data
			table.delete(delete);
			System.out.println("Column family deleted");
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
