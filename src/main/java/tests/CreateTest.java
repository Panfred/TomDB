package tests;

import uzh.tomdb.api.Connection;
import uzh.tomdb.api.Statement;
import uzh.tomdb.main.TomDB;


public class CreateTest {
    
    public static void main(String[] args) {
        test();
    }
    
    public static void test() {
      
                    TomDB.createLocalPeers(100);
                    Connection con = TomDB.getConnection();
                    Statement stmt = con.createStatement();
                    
                    long now = System.currentTimeMillis();
                    
                    
//                    stmt.execute("CREATE TABLE Salary (enum, name, salary) OPTIONS (blocksize:2)").start();
//                    stmt.execute("CREATE TABLE Salary (enum, name, salary) OPTIONS (index:salary, blocksize:2)").start();

                    stmt.execute("CREATE TABLE tabtest (id, value) OPTIONS (uniqueindex:value, blocksize:20, storage:insertionorder)").start();
                    
                    System.out.println("CREATE TABLE done in " + (System.currentTimeMillis() - now));

    }

}
