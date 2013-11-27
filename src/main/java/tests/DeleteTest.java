package tests;

import uzh.tomdb.api.Connection;
import uzh.tomdb.api.Statement;
import uzh.tomdb.main.TomDB;


public class DeleteTest {
    
    public static void main(String[] args) {
        test();
    }
    
    public static void test() {
      
                    Connection con = TomDB.getConnection();
                    Statement stmt = con.createStatement();
                    
                    long now = System.currentTimeMillis();

                    stmt.execute("DELETE FROM tabone WHERE id > 5 OPTIONS (tablescan)");
                    
                    stmt.start();
                    
                    System.out.println("UPDATE done in " + (System.currentTimeMillis() - now));

    }

}
