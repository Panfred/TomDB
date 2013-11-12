
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
 * @author Francesco Luminati
 */
public class ResultSet implements Runnable {
	private final Logger logger = LoggerFactory.getLogger(ResultSet.class);  
	private Map<String, Integer> columns;
	private Row current;
	private BlockingQueue<Row> rows = new ArrayBlockingQueue<>(1000);
	
	public void setColumns(Map<String, Integer> col) {
		this.columns = col;
	}
	
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
//		if(current == null) {
//			return false;
//		}
//		return true;
	}
	
	public String getString(int colIndex) {
		return current.getCol(colIndex);
		
	}
	
	public int getInt(int colIndex) {
		int ret = 0;
		try {
			ret = Integer.parseInt(current.getCol(colIndex));
		} catch (NumberFormatException e) {
			logger.error("Integer parsing error, use getString() instead", e);
		}
		return ret;
	}
	
	public String getString(String colLable) {
		return current.getCol(columns.get(colLable));
	}
	
	public int getInt(String colLable) {
		int ret = 0;
		try {
			ret = Integer.parseInt(current.getCol(columns.get(colLable)));
		} catch (NumberFormatException e) {
			logger.error("Integer parsing error, use getString() instead", e);
		}
		return ret;
	}
	
	public int getPrimaryKey() {
		return current.getRowID();	
	}
	
	public void addRow(Row row) {
		rows.add(row);
	}

	@Override
	public void run() {
		logger.debug("ResultSet Thread started!");
	}
}
