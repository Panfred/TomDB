package tests;

import uzh.tomdb.api.Connection;
import uzh.tomdb.api.ResultSet;
import uzh.tomdb.api.Statement;
import uzh.tomdb.main.TomDB;


public class SelectTest {
	
    
    public static void main(String[] args) {
        test();
    }
    
    public static void test() {
      
                    Connection con = TomDB.getConnection();
                    Statement stmt = con.createStatement();
                    
                    long now = System.currentTimeMillis();
                    
                    
//                    ResultSet results = stmt.executeQuery("SELECT * FROM Salary WHERE salary >= 6000");
//                    ResultSet results = stmt.executeQuery("SELECT * FROM Salary WHERE salary = 3500 OPTIONS (indexscan)");
                    
//                    while(results.next()) {
//                    	System.out.println("Employee "+ results.getInt("enum") + ": " + results.getString("name") + " ==> salary: " + results.getInt("salary"));
//                    }
                    
                    ResultSet results = stmt.executeQuery("SELECT value FROM tabtest");
                    while(results.next()) {
                    	System.out.println("ID: " + results.getInt(0));
                    }
                    
                    System.out.println("SELECT done in " + (System.currentTimeMillis() - now));
        
    }
    
}
