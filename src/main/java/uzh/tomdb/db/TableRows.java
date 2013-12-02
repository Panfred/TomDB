
package uzh.tomdb.db;

import java.io.Serializable;

/**
*
* MetaData class for the metadata about Rows.
* 
* Objects of this class are saved in the DHT to give access to the information to every peer.
*
* @author Francesco Luminati
*/
public class TableRows implements Serializable{
	private static final long serialVersionUID = 1L;
	/**
	 * Name of the table.
	 */
	private String tabName;
	/**
	 * Total number of rows saved in the table.
	 */
    private int rowsNum;
    /**
     * Size of a table block, defined with the blocksize option in CREATE TABLE.
     * Default: 100
     */
    private int blockSize;
    /**
     * Storage method (insertionorder/fullblocks), defined with the storage option in CREATE TABLE.
     */
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
    /**
     * Used by Insert to get a new row ID.
     * 
     * @return row id
     */
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
