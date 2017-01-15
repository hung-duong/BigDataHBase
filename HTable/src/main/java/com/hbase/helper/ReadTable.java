package com.hbase.helper;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class ReadTable {
	private static ReadTable readTable = null;
	private Connection conn = null;
	
	private ReadTable() throws IOException {
		conn = Connector.getInstance().getConnection();
	}
	
	public static ReadTable getInstance() throws IOException {
		if(readTable == null) {
			readTable = new ReadTable();
		}
		
		return readTable;
	}
	
	public void readCellData(String tableName, String rowName,
			String columnFamily, String columnQualifier) throws IOException {
		
		//Instantiating Table class
		Table table = conn.getTable(TableName.valueOf(tableName));
		
		try {
			Get g = new Get(Bytes.toBytes(rowName));
			
			Result result = table.get(g);
			byte[] value = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier));
			
			System.out.println("Row: " + rowName);
			System.out.println("Column family: " + columnFamily);
			System.out.println("Column qualifier: " + columnQualifier);
			System.out.println("Value: " + Bytes.toString(value));
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			//Close Table
			if(table != null) {
				table.close();
			}
		}
	}
	
	public void scanTable(String tableName) throws IOException {
		//Instantiating Table class
		Table table = conn.getTable(TableName.valueOf(tableName));
		
		ResultScanner resultScanner = null;
		
		try {
			Scan scan = new Scan();
			scan.addColumn(Bytes.toBytes("PersonalData"), Bytes.toBytes("FirstName"));
			scan.addColumn(Bytes.toBytes("PersonalData"), Bytes.toBytes("LastName"));
			scan.addColumn(Bytes.toBytes("PersonalData"), Bytes.toBytes("Address"));
			scan.addColumn(Bytes.toBytes("PersonalData"), Bytes.toBytes("City"));
			scan.addColumn(Bytes.toBytes("ProfessionalData"), Bytes.toBytes("Designation"));
			scan.addColumn(Bytes.toBytes("ProfessionalData"), Bytes.toBytes("Salary"));
			
			resultScanner = table.getScanner(scan);
			Result result = resultScanner.next();
			while(result != null) {
				byte[] firstName = result.getValue(Bytes.toBytes("PersonalData"), Bytes.toBytes("FirstName"));
				byte[] lastName = result.getValue(Bytes.toBytes("PersonalData"), Bytes.toBytes("LastName"));
				byte[] address = result.getValue(Bytes.toBytes("PersonalData"), Bytes.toBytes("Address"));
				byte[] city = result.getValue(Bytes.toBytes("PersonalData"), Bytes.toBytes("City"));
				byte[] designation = result.getValue(Bytes.toBytes("ProfessionalData"), Bytes.toBytes("Designation"));
				byte[] salary = result.getValue(Bytes.toBytes("ProfessionalData"), Bytes.toBytes("Salary"));
				
				System.out.println("First Name: " + Bytes.toString(firstName));
				System.out.println("Last Name: " + Bytes.toString(lastName));
				System.out.println("Address: " + Bytes.toString(address));
				System.out.println("City: " + Bytes.toString(city));
				System.out.println("Designation: " + Bytes.toString(designation));
				System.out.println("Salary: " + Bytes.toString(salary));
				System.out.println();
				
				result = resultScanner.next();
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			//Close Table
			if(table != null) {
				table.close();
			}
		}
	}
}
