package tests;

import uzh.tomdb.api.Connection;
import uzh.tomdb.api.Statement;
import uzh.tomdb.main.TomDB;


public class UpdateTest {
    
    public static void main(String[] args) {
        test();
    }
    
    public static void test() {
      
                    Connection con = TomDB.getConnection();
                    Statement stmt = con.createStatement();
                    
                    long now = System.currentTimeMillis();

                    stmt.execute("UPDATE tabone SET address = nuovo WHERE id > 5 OPTIONS (tablescan)");
                    
                    stmt.start();
                    
                    System.out.println("UPDATE done in " + (System.currentTimeMillis() - now));

    }

}
