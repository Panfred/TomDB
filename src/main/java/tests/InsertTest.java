package tests;

import uzh.tomdb.api.Connection;
import uzh.tomdb.api.ResultSet;
import uzh.tomdb.api.Statement;
import uzh.tomdb.main.TomDB;


public class InsertTest {

    
    public static void main(String[] args) {
        test();
    }
    
    public static void test() {
      
                    Connection con = TomDB.getConnection();
                    Statement stmt = con.createStatement();
                    
                    long now = System.currentTimeMillis();
                    
                    
//                    stmt.execute("INSERT INTO Salary VALUES (1, 'Bob', 8000)");
//                    stmt.execute("INSERT INTO Salary VALUES (2, 'Tom', 6000)");
//                    stmt.execute("INSERT INTO Salary VALUES (3, 'Ted', 4000)");
//                    stmt.execute("INSERT INTO Salary VALUES (4, 'Fred', 6000)");
//                    stmt.execute("INSERT INTO Salary VALUES (5, 'Frank', 4000)");
//                    stmt.execute("INSERT INTO Salary VALUES (6, 'Jon', 8000)");
//                    stmt.execute("INSERT INTO Salary VALUES (7, 'Tailor', 4000)");
//                    stmt.execute("INSERT INTO Salary VALUES (8, 'Sem', 3500)");
//                    stmt.start();
                    
                    	for (int i = 0; i < 1000; i++) {
                        	stmt.execute("INSERT INTO tabtest VALUES (ID, "+i+")");
                        }
                    	 stmt.start();
                    
//                    	stmt.execute("FETCH METADATA");
//                    	 
//                    	 ResultSet results = stmt.executeQuery("SELECT value FROM tabtest");
//                         while(results.next()) {
//                         	System.out.println("ID: " + results.getInt(0));
//                         }
//                    
                   
                    
                    System.out.println("INSERTS done in " + (System.currentTimeMillis() - now));

    }

}
