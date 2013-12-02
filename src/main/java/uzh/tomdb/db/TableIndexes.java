
package uzh.tomdb.db;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
*
* MetaData class for the metadata about Indexes.
* 
* Objects of this class are saved in the DHT to give access to the information to every peer.
*
* @author Francesco Luminati
*/
public class TableIndexes implements Serializable{

	private static final long serialVersionUID = 1L;
	/**
	 * Name of the table.
	 */
	private String tabName;
	/**
	 * DST Range for this table, defined with the dstrange option in CREATE TABLE.
	 * Default: 10000
	 */
	private int DSTRange;
	/**
	 * List of non-univocal indexes, defined with the index option in CREATE TABLE.
	 */
    private List<String> indexes = new ArrayList<>();
    /**
     * List of univocal indexes, defined with the univocalindex option in CREATE TABLE.
     */
    private List<String> univocalIndexes = new ArrayList<>();
    /**
     * MetaData for every index in the lists. 
     * Contains information about the minimal and maximal value saved in the index, used to calculate the ranges for the DST.
     */
    private Map<String, IndexMeta> indexMetas = new HashMap<>();
    
    public TableIndexes (String tabName, int DSTRange) {
    	this.tabName = tabName;
    	this.DSTRange = DSTRange;
    }
    
    public String getTabName() {
        return tabName;
    }
    
    public int getDSTRange() {
    	return DSTRange;
    }

	public List<String> getIndexes() {
		return indexes;
	}

	public void setIndexes(List<String> indexes) {
		this.indexes = indexes;
		for (int i = 0; i < indexes.size(); i++) {
			indexMetas.put(indexes.get(i), new IndexMeta());
		}
	}

	public List<String> getUnivocalIndexes() {
		return univocalIndexes;
	}

	public void setUnivocalIndexes(List<String> uniqueIndexes) {
		this.univocalIndexes = uniqueIndexes;
		for (int i = 0; i < uniqueIndexes.size(); i++) {
			indexMetas.put(uniqueIndexes.get(i), new IndexMeta());
		}
	}
	
	/**
	 * Used to update the min/max value for every new entry in the index.
	 * 
	 * @param index name
	 * @param value
	 */
	public void setMinMax(String index, int value) {
		IndexMeta iMeta = indexMetas.get(index);
		if(iMeta.initialized) {
			if (value < iMeta.min) {
				iMeta.min = value;
			}
			else if (value > iMeta.max) {
				iMeta.max = value;
			}
		} else {
			iMeta.initialized = true;
			iMeta.min = value;
			iMeta.max = value;
		}
		
	}
	
	public int getMin(String index) {
		return indexMetas.get(index).min;
	}
	
	public int getMax(String index) {
		return indexMetas.get(index).max;
	}
	
	private class IndexMeta implements Serializable {
		private static final long serialVersionUID = 1L;
		private boolean initialized = false;
		private int min = 0;
		private int max = 0;
		@Override
		public String toString() {
			return "IndexMeta [min=" + min + ", max=" + max + "]";
		}
		
	}

	@Override
	public String toString() {
		return "TableIndexes [tabName=" + tabName + ", DSTRange=" + DSTRange
				+ ", indexes=" + indexes
				+ ", uniqueIndexes=" + univocalIndexes + ", indexMetas="
				+ indexMetas + "]";
	}

}
