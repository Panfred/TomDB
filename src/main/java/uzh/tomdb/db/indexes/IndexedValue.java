
package uzh.tomdb.db.indexes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 * MetaData class containing the indexed value and all the row IDs that it is pointing to.
 * 
 * @author Francesco Luminati
 */
public class IndexedValue implements Serializable{

	private static final long serialVersionUID = 1L;
	private int indexedVal;
	private List<Integer> rowIds = new ArrayList<>();
	
	public IndexedValue(int indexedVal, List<Integer> rowIds) {
		this.indexedVal = indexedVal;
		this.rowIds = rowIds;
	}
	
	public IndexedValue(int indexedVal, int rowId) {
		this.indexedVal = indexedVal;
		rowIds.add(rowId);
	}
	
	public void addRowId(int rowId) {
		rowIds.add(rowId);
	}

	public int getIndexedVal() {
		return indexedVal;
	}

	public List<Integer> getRowIds() {
		return rowIds;
	}

	@Override
	public String toString() {
		return "IndexedValue [indexedVal=" + indexedVal + ", rowIds=" + rowIds
				+ "]";
	}

}
