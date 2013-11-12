
package uzh.tomdb.db.operations.helpers;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Francesco Luminati
 */
public class Row implements Serializable{
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final int id;
    private List<String> row;
    
    public Row(int id) {
        this.id = id;
        row = new ArrayList<>();
    }
    
    public Row(int id, List<String> row) {
        this.id = id;
        this.row = row;
    }

    public int getRowID() {
        return id;
    }
    
    public List<String> getRow() {
        return row;
    }
    
    public String getCol(int colId) {
        return row.get(colId);
    }

    public void setRow(List<String> row) {
        this.row = row;
    }
    
    public void setCol(int colId, String value) {
        row.add(colId, value);
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
