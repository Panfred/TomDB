
package uzh.tomdb.db.operations.helpers;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Francesco Luminati
 */
public class Row implements Serializable{
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final String tabName;
	private final int id;
    private List<String> row;
    private Map<String, Integer> columns;
    
    public Row(int id) {
    	this.id = id;
    	tabName = null;
    }
    
    public Row(String tabName, int id, Map<String, Integer> col) {
    	this.tabName = tabName;
        this.id = id;
        row = new ArrayList<>();
        columns = col;
    }
    
    public Row(String tabName, int id, List<String> row, Map<String, Integer> col) {
    	this.tabName = tabName;
        this.id = id;
        this.row = row;
        this.columns = col;
    }

    public String getTabName() {
    	return tabName;
    }
    
    public int getRowID() {
        return id;
    }
    
    public List<String> getRow() {
        return row;
    }
    
    public Map<String, Integer> getColumns() {
    	return columns;
    }
    
    public String getCol(int colId) {
        return row.get(colId);
    }
    
    public String getCol(String col) {
        return row.get(columns.get(col));
    }

    public void setRow(List<String> row) {
        this.row = row;
    }
    
    public void setCol(int colId, String value) {
        row.add(colId, value);
    }
    
    public void setCol(String col, String value) {
        row.add(columns.get(col), value);
    }
    
    @Override
    public String toString() {
        String temp = "RowId:"+ id + ":::"; 
        for(int i = 0; i < row.size(); i++) {
            temp += "Col" + (i+1) + ":" + row.get(i) + ";";
        }
        return temp;
    }
    
}
