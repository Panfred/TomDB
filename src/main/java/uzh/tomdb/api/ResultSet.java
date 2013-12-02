
package uzh.tomdb.api;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uzh.tomdb.db.operations.helpers.Row;

/**
 * 
 * RedsultSet class (like JDBC)
 * 
 * Implements a BlockingQueue to wait for the results coming from asynchronous operations.
 *
 * @author Francesco Luminati
 */
public class ResultSet implements Runnable {
	private final Logger logger = LoggerFactory.getLogger(ResultSet.class);  
	private Map<String, Integer> columns;
	private Row current;
	private BlockingQueue<Row> rows = new ArrayBlockingQueue<>(1000);
	
	/**
	 * Sets the columns of the result set.
	 * 
	 * @param columns
	 */
	public void setColumns(Map<String, Integer> col) {
		this.columns = col;
	}
	
	/**
	 * Returns true until all the results have arrived.
	 * 
	 * To be used in a WHILE loop.
	 * 
	 * The BlockingQueue time out after 20 seconds.
	 * 
	 * @return
	 */
	public boolean next() {	
		try {
			current = rows.poll(20, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			logger.error("ResultSet Queue Interrupted", e);
		}
		if(current != null && current.getRowID() >= 0) {
			return true;
		}
		return false;
	}
	
	/**
	 * Return the String at the given column index.
	 * 
	 * @param colIndex
	 * @return String
	 */
	public String getString(int colIndex) {
		return current.getCol(colIndex);	
	}
	
	/**
	 * Return an int at the given column index.
	 * 
	 * @param colIndex
	 * @return int
	 * @throws NumberFormatException if the string can not be parsed as an int.
	 */
	public int getInt(int colIndex) {
		int ret = 0;
		try {
			ret = Integer.parseInt(current.getCol(colIndex));
		} catch (NumberFormatException e) {
			logger.error("Integer parsing error, use getString() instead", e);
		}
		return ret;
	}
	
	/**
	 * Return the String for the given column name.
	 * 
	 * @param colLable
	 * @return String
	 */
	public String getString(String colLable) {
		return current.getCol(columns.get(colLable));
	}
	
	/**
	 * Return an int for the given column name.
	 * @param colLable
	 * @return int
	 * @throws NumberFormatException if the string can not be parsed as an int.
	 */
	public int getInt(String colLable) {
		int ret = 0;
		try {
			ret = Integer.parseInt(current.getCol(columns.get(colLable)));
		} catch (NumberFormatException e) {
			logger.error("Integer parsing error, use getString() instead", e);
		}
		return ret;
	}
	
	/**
	 * Returns the internal Row ID.
	 * 
	 * @return key
	 */
	public int getRowID() {
		return current.getRowID();	
	}
	
	/**
	 * Returns a string representation of the current row.
	 * 
	 * @return String
	 */
	public String rowToString() {
		return current.toString();
	}
	
	/**
	 * Used internally to add rows to the result set.
	 * 
	 * @param row
	 */
	public void addRow(Row row) {
		rows.add(row);
	}

	@Override
	public void run() {
		logger.debug("ResultSet Thread started!");
	}
}
