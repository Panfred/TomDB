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
                    
                    ResultSet results = stmt.executeQuery("SELECT * FROM tabone");
                    while(results.next()) {
                    	System.out.println("name: " + results.getString("nameone")+" address: " + results.getString("address"));
                    }
                    
                    //JOIN
//                    ResultSet results = stmt.executeQuery("SELECT * FROM tabone, tabtwo WHERE tabone.id = tabtwo.idtwo OPTIONS (indexscan)");
//                    while(results.next()) {
//                    	System.out.println("name: " + results.getString("nameone")+" salary: " + results.getString("salary"));
//                    }
                    
                    
                    
                    System.out.println("SELECT done in " + (System.currentTimeMillis() - now));
        
    }
    
}
