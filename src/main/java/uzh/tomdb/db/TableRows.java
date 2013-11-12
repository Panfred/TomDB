
package uzh.tomdb.db;

import java.io.Serializable;

/**
 *
 * @author Francesco Luminati
 */
public class TableRows implements Serializable{
	private static final long serialVersionUID = 1L;
	private String tabName;
    private int rowsNum;
    private int blockSize;
    private String storage;

    public TableRows(String tabName, int blockSize, String storage) {
        this.tabName = tabName;
        this.blockSize = blockSize;
        this.storage = storage;
    }

    public String getTabName() {
        return tabName;
    }

    public int getRowsNum() {
        return rowsNum;
    }

    public int incrementAndGetRowsNum() {
        return ++rowsNum;
    }
    
    public int getBlockSize() {
        return blockSize;
    }

	public String getStorage() {
		return storage;
	}

	@Override
	public String toString() {
		return "TableRows [tabName=" + tabName + ", rowsNum=" + rowsNum
				+ ", blockSize=" + blockSize + 
				", storage=" + storage + "]";
	}
    
}
