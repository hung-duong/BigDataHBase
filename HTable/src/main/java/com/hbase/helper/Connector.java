package com.hbase.helper;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class Connector {
	private static Connector connector = null;
	private static Connection connection = null;
	
	private Connector() throws IOException {
		if(connection == null) {
			Configuration conf = HBaseConfiguration.create();
			connection = ConnectionFactory.createConnection(conf);
		}
	}
	
	public static Connector getInstance() throws IOException {
		if(connector == null) {
			connector = new Connector();
		}
		
		return connector;
	}
	
	public Connection getConnection() {
		return connection;
	}
	
	public void close() throws IOException {
		System.out.println("CLose connection");
		connection.close();
	}
}
