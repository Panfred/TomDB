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
                    
                    ResultSet results = stmt.executeQuery("SELECT * FROM tabtwo");
                    while(results.next()) {
                    	System.out.println("name: " + results.getString("nametwo")+" salary: " + results.getString("salary"));
                    }
                    
                    //JOIN
//                    ResultSet results = stmt.executeQuery("SELECT * FROM tabone, tabtwo WHERE tabone.nameone = tabtwo.nametwo AND id > 90");
//                    while(results.next()) {
//                    	System.out.println("name: " + results.getString("nameone")+" salary: " + results.getString("salary"));
//                    }
                    
                    
                    
                    System.out.println("SELECT done in " + (System.currentTimeMillis() - now));
        
    }
    
}
