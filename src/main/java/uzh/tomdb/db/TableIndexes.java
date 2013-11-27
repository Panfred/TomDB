package uzh.tomdb.db;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableIndexes implements Serializable{

	private static final long serialVersionUID = 1L;
	private String tabName;
	private int DSTRange;
	private String primaryKey;
    private List<String> indexes = new ArrayList<>();
    private List<String> uniqueIndexes = new ArrayList<>();
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
    
    public String getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	public boolean hasPrimaryKey() {
		if (primaryKey != null) {
			return true;
		} else {
			return false;
		}
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

	public List<String> getUniqueIndexes() {
		return uniqueIndexes;
	}

	public void setUniqueIndexes(List<String> uniqueIndexes) {
		this.uniqueIndexes = uniqueIndexes;
		for (int i = 0; i < uniqueIndexes.size(); i++) {
			indexMetas.put(uniqueIndexes.get(i), new IndexMeta());
		}
	}
	
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
				+ ", primaryKey=" + primaryKey + ", indexes=" + indexes
				+ ", uniqueIndexes=" + uniqueIndexes + ", indexMetas="
				+ indexMetas + "]";
	}

}
