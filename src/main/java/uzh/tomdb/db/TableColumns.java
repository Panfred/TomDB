
package uzh.tomdb.db;

import java.io.Serializable;
import java.util.Map;

import uzh.tomdb.parser.MalformedSQLQuery;

/**
 *
 * MetaData class for the metadata about Columns.
 * 
 * Objects of this class are saved in the DHT to give access to the information to every peer.
 *
 * @author Francesco Luminati
 */
public class TableColumns implements Serializable{
    
	private static final long serialVersionUID = 1L;
	/**
	 * Name of the table.
	 */
	private String tabName;
	/**
	 * Columns in the table.
	 */
    private Map<String, Integer> columns;

    public TableColumns(String tabName, Map<String, Integer> columns) {
        this.tabName = tabName;
        this.columns = columns;
    }

    public String getTabName() {
        return tabName;
    }

    public Map<String, Integer> getColumns() {
        return columns;
    }
	
	public int getNumOfCols() {
		return columns.size();
	}
	
	public int getColumnId(String colName) throws MalformedSQLQuery {	
		if (!columns.containsKey(colName)) {
			throw new MalformedSQLQuery("SQL error, selected column does not exist.");
		}
		return columns.get(colName);
	}

	@Override
	public String toString() {
		return "TableColumns [tabName=" + tabName + ", columns=" + columns
				+ "]";
	}

}