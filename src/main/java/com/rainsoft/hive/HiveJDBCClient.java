package com.rainsoft.hive;

import java.sql.*;

public class HiveJDBCClient {
	private static String DRIVERNAME = "org.apache.hive.jdbc.HiveDriver";
	 
	public static void main(String[] args) throws SQLException {
		  //加载驱动类
	      try {
	      Class.forName(DRIVERNAME);
	    } catch (ClassNotFoundException e) {
	      // TODO Auto-generated catch block
	      e.printStackTrace();
	      System.exit(1);
	    }
	      
	    //replace "hive" here with the name of the user the queries should runImport as
		Connection con = DriverManager.getConnection("jdbc:hive2://nn1.hadoop.com:10000/yuncai", "root", "rainsoft");
	    Statement stmt = con.createStatement();
	    String sql = "select * from user limit 3";
	    System.out.println("Running: " + sql);
	    //query
	    ResultSet res = stmt.executeQuery(sql);
	    while (res.next()) {
	      System.out.println(res.getString(1) + "\t" + res.getString(2) + "\t" + res.getString(3));
	    }
	  }
}
