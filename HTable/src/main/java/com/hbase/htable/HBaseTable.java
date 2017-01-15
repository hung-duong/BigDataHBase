package com.hbase.htable;

import java.io.IOException;

import com.hbase.helper.Connector;
import com.hbase.helper.DeleteTable;
import com.hbase.helper.DropTable;
import com.hbase.helper.InitTable;
import com.hbase.helper.InsertTable;
import com.hbase.helper.ReadTable;

public class HBaseTable {
	
	public static void main(String[] args) throws IOException {
	
		String tableName = "Person";
		String[] columnsFamily = {"PersonalData", "ProfessionalData"};
		
		//Creating new table
		InitTable initTable = InitTable.getInstance();
		initTable.initTable(tableName, columnsFamily);
		
		InsertTable insertTable = InsertTable.getInstance();
		
		//Created first row data
		String row01 = "row_01";
		insertTable.createData(tableName, row01, columnsFamily[0], "FirstName", "Mickey");
		insertTable.createData(tableName, row01, columnsFamily[0], "LastName", "Mouse");	
		insertTable.createData(tableName, row01, columnsFamily[0], "Address", "123 Fantasty way");
		insertTable.createData(tableName, row01, columnsFamily[0], "City", "Anaheim");
		insertTable.createData(tableName, row01, columnsFamily[1], "Designation", "Manager");
		insertTable.createData(tableName, row01, columnsFamily[1], "Salary", "50.000");
		
		//Created second row data
		String row02 = "row_02";
		insertTable.createData(tableName, row02, columnsFamily[0], "FirstName", "Bat");
		insertTable.createData(tableName, row02, columnsFamily[0], "LastName", "Man");
		insertTable.createData(tableName, row02, columnsFamily[0], "Address", "321 Cavern Ave");
		insertTable.createData(tableName, row02, columnsFamily[0], "City", "Paradise");
		insertTable.createData(tableName, row02, columnsFamily[1], "Designation", "Sr. Engineer");
		insertTable.createData(tableName, row02, columnsFamily[1], "Salary", "30.000");
	
		//Display all rows in table
		System.out.println("========Get all records after inserting==========");	
		ReadTable readTable = ReadTable.getInstance();
		readTable.scanTable(tableName);
		System.out.println();
		
		//Delete first row
		DeleteTable deleteRowTable = DeleteTable.getInstance();
		deleteRowTable.deleteRow(tableName, row01);
		
		System.out.println("========Delete first row=========");
		readTable.scanTable(tableName);
		System.out.println();
		
		//Deleting cell in table
		System.out.println("========Delete the first name value of first row======");
		deleteRowTable.deleteCell(tableName, row02, columnsFamily[0], "FirstName");
		readTable.scanTable(tableName);
		System.out.println();
		
		//Delete column family in table
		System.out.println("=======Delete the Professional Data column family of first row=====");
		deleteRowTable.deleteFamily(tableName, row02, columnsFamily[1]);
		readTable.scanTable(tableName);
		System.out.println();
		
		//Deleting table
		System.out.println("=======Delete the table======");
		DropTable dropTable = DropTable.getInstance();
		dropTable.dropTable(tableName);
		System.out.println();
		
		//Close the connection
		System.out.println("=======Close the connection======");
		Connector.getInstance().close();
	}
}
