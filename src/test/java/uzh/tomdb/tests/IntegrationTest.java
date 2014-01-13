package uzh.tomdb.tests;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import uzh.tomdb.api.Connection;
import uzh.tomdb.api.ResultSet;
import uzh.tomdb.api.Statement;
import uzh.tomdb.main.TomDB;

@FixMethodOrder(MethodSorters.NAME_ASCENDING) 
public class IntegrationTest {
	
	static Statement stmt;

	@BeforeClass
	public static void setUp() throws Exception {
		TomDB.createLocalPeers(100, false);
		Connection con = TomDB.getConnection();
		stmt = con.createStatement();
	}

	@Test
	public void a_createTable() {
		assertTrue(stmt.execute("CREATE TABLE testinsor (id, index, text) OPTIONS (storage:insertionorder, univocalindex:id, index:index, blocksize:5, dstrange:200)"));
		assertTrue(stmt.execute("CREATE TABLE testfullbl (id2, index2, text2) OPTIONS (storage:fullblocks, univocalindex:id2, index:index2, blocksize:5, dstrange:200)"));
		stmt.start();
	}
	
	@Test
	public void b_insert() {
		int uniqueVal = 0;
		String text = "";
		for (int i = 1; i <= 100; i++) {
			if(i%2 == 0) {
				uniqueVal = i;
				text = "someText";
			} else {
				text = "otherText";
			}
			assertTrue(stmt.execute("INSERT INTO testinsor VALUES ("+i+","+uniqueVal+","+text+")"));
		}
		for (int i = 1; i <= 100; i++) {
			if(i%2 == 0) {
				uniqueVal = i;
			}
			assertTrue(stmt.execute("INSERT INTO testfullbl VALUES ("+i+","+uniqueVal+",'someText')"));
		}
		stmt.start();
	}
	
	@Test
	public void c_select() {
		ResultSet results = stmt.executeQuery("SELECT id, text FROM testinsor WHERE text = 'otherText'");
		Set<Integer> res = new HashSet<>();
		while (results.next()) {
//			System.out.println(results.rowToString());
			res.add(results.getInt(0));
		}
		assertEquals(50, res.size());
		
		ResultSet results2 = stmt.executeQuery("SELECT * FROM testfullbl WHERE id2 > 90 OR index2 <= 50 OPTIONS (indexscan)");
		Set<Integer> res2 = new HashSet<>();
		while (results2.next()) {
//			System.out.println(results2.rowToString());
			res2.add(results2.getInt(0));
		}
		assertEquals(60, res2.size());
	}
	
	@Test
	public void d_update() {
		assertTrue(stmt.execute("UPDATE testinsor SET text='updated text' WHERE id >= 80 AND id < 90 OPTIONS (tablescan)"));
		assertTrue(stmt.execute("UPDATE testinsor SET text='reupdated text indexscan' WHERE id > 90 OPTIONS (indexscan)"));
		stmt.start();
	}
	
	@Test
	public void e_selectUpdate() throws InterruptedException {
		Thread.sleep(1000);
		ResultSet results = stmt.executeQuery("SELECT * FROM testinsor WHERE id >= 80 OPTIONS (indexscan)");
		int counter = 0;
		while (results.next()) {
			if (results.getString("text").equals("updated text") || results.getString("text").equals("reupdated text indexscan") ) {
				counter++;
			}
//			System.out.println("TABLE:UPDATE:"+results.rowToString());
		}
		assertEquals(20, counter);
	}
	
	@Test
	public void f_delete() {
		assertTrue(stmt.execute("DELETE FROM testinsor WHERE id <= 10 OPTIONS (tablescan)"));
		assertTrue(stmt.execute("DELETE FROM testfullbl WHERE id2 <= 10 OPTIONS (indexscan)"));
		stmt.start();
	}
	
	@Test
	public void g_insertDelete() {
		
		for (int i = 101; i <= 110; i++) {
			assertTrue(stmt.execute("INSERT INTO testinsor VALUES ("+i+","+i+",'reinsertedText')"));
		}
		for (int i = 101; i <= 110; i++) {
			assertTrue(stmt.execute("INSERT INTO testfullbl VALUES ("+i+","+i+",'reinsertedText')"));
		}
		stmt.start();
	}
	
	@Test
	public void h_selectDelete() {
		
		ResultSet results = stmt.executeQuery("SELECT * FROM testinsor WHERE id > 100 OPTIONS (indexscan)");
		int counter = 0;
		while (results.next()) {
			if (results.getRowID() > 100) {
				counter++;
			}
//			System.out.println("TABLE:testinsor:"+results.rowToString());
		}
		assertEquals(10, counter);
		
		ResultSet results2 = stmt.executeQuery("SELECT * FROM testfullbl WHERE id2 > 100 OPTIONS (indexscan)");
		int counter2 = 0;
		while (results2.next()) {
			if (results2.getRowID() <= 10) {
				counter2++;
			}
//			System.out.println("TABLE:testfullbl:"+results2.rowToString());
		}
		assertEquals(10, counter2);
	}
	
	@Test
	public void i_join1() {
		ResultSet results = stmt.executeQuery("SELECT id FROM testinsor, testfullbl WHERE testinsor.index = testfullbl.index2 OPTIONS (tablescan)");
		int counter = 0;
		while (results.next()) {
			counter++;
			System.out.println("Join1::"+results.rowToString());
		}
		assertEquals(188, counter);
	}
	
	@Test
	public void l_join2() {
		ResultSet results2 = stmt.executeQuery("SELECT * FROM testinsor, testfullbl WHERE testinsor.index = testfullbl.index2 AND id > 50 OPTIONS (indexscan)");
		int counter2 = 0;
		while (results2.next()) {
			counter2++;
			System.out.println("Join2::"+results2.rowToString());
		}
		assertEquals(109, counter2);
	}
	
}
