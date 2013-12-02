package uzh.tomdb.db.operations;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uzh.tomdb.api.ResultSet;
import uzh.tomdb.db.TableColumns;
import uzh.tomdb.db.TableIndexes;
import uzh.tomdb.db.TableRows;
import uzh.tomdb.db.operations.engines.QueryExecuter;
import uzh.tomdb.db.operations.helpers.Row;
import uzh.tomdb.db.operations.helpers.TempResults;
import uzh.tomdb.db.operations.helpers.WhereCondition;
import uzh.tomdb.p2p.DBPeer;
import uzh.tomdb.parser.MalformedSQLQuery;

/**
*
* SELECT SQL operation.
*
* @author Francesco Luminati
*/
public class Select extends Operation implements Operations {
	private final Logger logger = LoggerFactory.getLogger(Select.class);
	/**
	 * The columns to be selected.
	 */
	private List<String> columns;
	/**
	 * Where conditions for the SELECT operation.
	 */
	private List<WhereCondition> whereConditions;
	/**
	 * allColumns correspond to the SELECT *.
	 */
	private boolean allColumns = false;
	/**
     * ResultSet to return the results through the API.
     */
	private ResultSet resultSet;
	/**
     * Scan type (tablescan/indexscan) defined in the OPTIONS statement.
     */
	private String scanType;
	/**
	 * List of table names used for the JOIN.
	 */
	private List<String> tabNames;
	private TempResults tmpRes;
	
	/**
	 * For a normal SELECT.
	 */
	public Select(boolean allColumns, String tabName, List<String> columns, List<WhereCondition> conditions, String scanType) {
		super();
		super.tabName = tabName;
		super.tabKey = Number160.createHash(tabName);
		this.allColumns = allColumns;
		this.columns = columns;
		this.whereConditions = conditions;
		this.scanType = scanType;
	}
	
	/**
	 * For the JOIN.
	 */
	public Select(boolean allColumns, List<String> tabNames, List<String> columns, List<WhereCondition> conditions, String scanType) {
		super();
		this.tabNames = tabNames;
		this.allColumns = allColumns;
		this.columns = columns;
		this.whereConditions = conditions;
		this.scanType = scanType;
	}
	
	/**
	 * For internal SELECTs, used by the JOIN.
	 */
	public Select(String tabName, TableRows tr, TableIndexes ti, TableColumns tc) {
		super();
		super.tabName = tabName;
		super.tr = tr;
		super.ti = ti;
		super.tc = tc;
	}
	
	/**
	 * For internal SELECTs used by UPDATE, DELETE.
	 */
	public Select(String tabName, TableRows tr, TableIndexes ti, TableColumns tc, List<WhereCondition> conditions, String scanType, TempResults tmpRes) {
		super();
		super.tabName = tabName;
		super.tabKey = Number160.createHash(tabName);
		this.allColumns = true;
		this.columns = null;
		this.whereConditions = conditions;
		this.scanType = scanType;
		this.tmpRes = tmpRes;
		super.tr = tr;
		super.ti = ti;
		super.tc = tc;
	}
	
	/**
	 * Creates a new ResultSet if necessary, sets the table MetaData if necessary and starts the QueryExecuter.
	 * The QueryExecuter gets this object and give it to the ConditionsHandler.
	 */
	@Override
	public void init() {
		try {
			if (getTabNames() != null) {
				resultSet = new ResultSet();
				new QueryExecuter(this);
			} else if (tmpRes != null){
				new QueryExecuter(this);
			} else {
			
				Map<Number160, Data> tabColumns = DBPeer.getTabColumns();
				Map<Number160, Data> tabRows = DBPeer.getTabRows();
				Map<Number160, Data> tabIndexes = DBPeer.getTabIndexes();

				tc = (TableColumns) tabColumns.get(tabKey).getObject();
				tr = (TableRows) tabRows.get(tabKey).getObject();
				ti = (TableIndexes) tabIndexes.get(tabKey).getObject();

				resultSet = new ResultSet();

				new QueryExecuter(this);
			}
		} catch (ClassNotFoundException | IOException e) {
			logger.error("Data error", e);
		} catch (MalformedSQLQuery e) {
			logger.error("SQL error", e);
		}
	}
	
	/**
	 * Gets the rows from the ConditionsHandler.
	 * 
	 * @param row
	 */
	public void addToResultSet(Row row) {
		if (tmpRes != null) {
			tmpRes.addRow(row);
		} else {
			resultSet.addRow(row);
		}
	}
	
	public void setResultSetColumns(Map<String, Integer> col) {
		if (resultSet != null) {
			resultSet.setColumns(col);
		}	
	}

	public ResultSet getResultSet() {
		new Thread(resultSet).start();
		return resultSet;
	}

	public List<String> getColumns() {
		return columns;
	}

	public List<WhereCondition> getWhereConditions() {
		return whereConditions;
	}
	
	public String getScanType() {
		return scanType;
	}
	
	public boolean isAllColumns() {
		return allColumns;
	}

	public List<String> getTabNames() {
		return tabNames;
	}

	@Override
	public String toString() {
		return "Select [columns=" + columns + ", whereConditions="
				+ whereConditions + ", allColumns=" + allColumns
				+ ", resultSet=" + resultSet + ", scanType=" + scanType
				+ ", tabNames=" + tabNames + "]";
	}

}
