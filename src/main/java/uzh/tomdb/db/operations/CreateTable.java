
package uzh.tomdb.db.operations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uzh.tomdb.db.TableColumns;
import uzh.tomdb.db.TableIndexes;
import uzh.tomdb.db.TableRows;
import uzh.tomdb.p2p.DBPeer;
import uzh.tomdb.parser.MalformedSQLQuery;

/**
 *
 * CREATE TABLE SQL operation.
 *
 * @author Francesco Luminati
 */
public class CreateTable extends Operation implements Operations{
    private final Logger logger = LoggerFactory.getLogger(CreateTable.class);
    /**
     * Columns of the new table.
     */
    private List<String> columns;
    /**
     * Options for the new table, including indexes, storage method, block size and DST range.
     */
    private Map<String, String> options;
    private List<String> indexes;
    private List<String> univocalIndexes;
    
    public CreateTable(String tabName, List<String> columns) {
    	super();
        super.tabName = tabName;
        super.tabKey = Number160.createHash(tabName);
        this.columns = columns;
    }
    
    public CreateTable(String tabName, List<String> columns, List<String> options) throws MalformedSQLQuery {
    	super();
        super.tabName = tabName;
        super.tabKey = Number160.createHash(tabName);
        this.columns = columns;
        indexes = new ArrayList<>();
        univocalIndexes = new ArrayList<>();
        this.options = parseOptions(options);
    }
    
    /**
     * Initializes new table MetaData objects and inserts the information in them.
     * The MetaData objects are then added to the DBPeer.
     */
    @Override
    public void init() {

		TableColumns tColumns = new TableColumns(tabName, getColumns());
		TableRows tRows = new TableRows(tabName, getBlockSize(), getStorage());
		TableIndexes tIndexes = new TableIndexes(tabName, getDSTRange());
		
		if (indexes != null) {
			tIndexes.setIndexes(indexes);
		}
		
		if (univocalIndexes != null) {
			tIndexes.setUnivocalIndexes(univocalIndexes);
		}

		try {
			DBPeer.addTabColumn(tabKey, new Data(tColumns));
			DBPeer.addTabRow(tabKey, new Data(tRows));
			DBPeer.addTabIndex(tabKey, new Data(tIndexes));	
		} catch (IOException e) {
			logger.error("Data error", e);
		}
	
    }

    private Map<String, Integer> getColumns() {
        Map<String, Integer> map = new LinkedHashMap<>();
        for (int i = 0; i < columns.size(); i++) {
        	map.put(columns.get(i), i);
        }
        return map;
    }

    private int getBlockSize() {
        if (options != null && options.containsKey("blocksize")) {
            return Integer.parseInt(options.get("blocksize"));
        }
        //else return default value
        return 100;
    }
    
    private int getDSTRange() {
        if (options != null && options.containsKey("dstrange")) {
            return Integer.parseInt(options.get("dstrange"));
        }
        //else return default value
        return 10000;
    }
    
    private String getStorage() {
    	if (options != null && options.containsKey("storage")) {
    		return options.get("storage");   		
    	}
    	//else return default value
    	return "insertionorder";
    }
    
    /**
     * Internal SQL parser for the custom OPTIONS statement.
     * 
     * @param options
     */
	private Map<String, String> parseOptions(List<String> options) throws MalformedSQLQuery {
		Map<String, String> retOptions = new HashMap<>();

		for (int i = 0; i < options.size(); i++) {
			String[] spl = options.get(i).split("\\:");
			switch (spl[0]) {
			case "blocksize":
				retOptions.put("blocksize", spl[1]);
				break;
			case "dstrange":
				retOptions.put("dstrange", spl[1]);
				break;
			case "storage":
				switch (spl[1]) {
					case "insertionorder":
						retOptions.put("storage", spl[1]);
						break;
					case "fullblocks":
						retOptions.put("storage", spl[1]);
						break;
					default:
						throw new MalformedSQLQuery("CREATE TABLE Options SQL error with STORAGE.");
				}
				break;
			case "primarykey":
				retOptions.put("primarykey", spl[1]);
				break;
			case "index":
				indexes.add(spl[1]);
				break;
			case "univocalindex":
				univocalIndexes.add(spl[1]);
				break;
			default:
				throw new MalformedSQLQuery("CREATE TABLE Options SQL error.");
			}
		}

		return retOptions;
	}

	@Override
	public String toString() {
		return "CreateTable [tabName=" + tabName
				+ ", columns=" + columns + ", options=" + options
				+ ", indexes=" + indexes + ", univocalIndexes=" + univocalIndexes
				+ "]";
	}
    
}
