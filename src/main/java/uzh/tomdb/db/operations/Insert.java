
package uzh.tomdb.db.operations;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uzh.tomdb.api.Statement;
import uzh.tomdb.db.TableColumns;
import uzh.tomdb.db.TableIndexes;
import uzh.tomdb.db.TableRows;
import uzh.tomdb.db.indexes.IndexHandler;
import uzh.tomdb.db.operations.engines.FreeBlocksHandler;
import uzh.tomdb.db.operations.helpers.Block;
import uzh.tomdb.db.operations.helpers.Row;
import uzh.tomdb.db.operations.helpers.Utils;
import uzh.tomdb.p2p.DBPeer;
import uzh.tomdb.parser.MalformedSQLQuery;

/**
*
* INSERT SQL operation.
*
* @author Francesco Luminati
*/
public class Insert extends Operation implements Operations{
    private final Logger logger = LoggerFactory.getLogger(Insert.class);
    /**
     * List of columns to insert to.
     */
    private List<String> columns;
    /**
     * The values that are inserted in the given columns.
     */
    private List<String> values;
	private FreeBlocksHandler freeBlocksHandler;
	private Map<String, IndexHandler> indexHandler;
    private List<String> indexes;
    private List<String> uniqueIndexes;
    private boolean done = false;
    
	public Insert(String tabName, List<String> values) {
        super();
		super.tabName = tabName;
        super.tabKey = Number160.createHash(tabName);
        this.values = values;
    }
    
    public Insert(String tabName, List<String> values, List<String> columns) {
    	super();
        super.tabName = tabName;
        super.tabKey = Number160.createHash(tabName);
        this.columns = columns;
        this.values = values;
    }
    
    /**
     * Gets the freeblocks handler and the table metadata from the OperationEngine.
     * Do the insertion for insertionorder or fullblocks methods.
     * @param freeBlocksHandler
     * @param tr
     * @param ti
     */
	public void init(FreeBlocksHandler freeBlocksHandler, Map<String, IndexHandler> ih, TableRows tr, TableIndexes ti) {
		this.freeBlocksHandler = freeBlocksHandler;
		this.indexHandler = ih;
		Map<Number160, Data> tabColumns = DBPeer.getTabColumns();
		
		this.tr = tr;
		this.ti = ti;
		
		try {
			tc = (TableColumns) tabColumns.get(tabKey).getObject();
			indexes = ti.getIndexes();
			uniqueIndexes = ti.getUnivocalIndexes();

			switch (tr.getStorage()) {
				case "insertionorder":
					putInsertionOrder();
					break;
				case "fullblocks":
					putFullBlocks();
					break;
			}

		} catch (ClassNotFoundException | IOException ex) {
			logger.error("Data error", ex);
		} catch (MalformedSQLQuery ex) {
			logger.error("SQL error", ex);
		}

	}
	
	/**
	 * Insert the row always in the last table block.
	 */
	private void putInsertionOrder() throws IOException, MalformedSQLQuery, ClassNotFoundException {

    	int rowId = tr.incrementAndGetRowsNum();
    	int blockSize = tr.getBlockSize();

    	Row row = getRow(rowId);
    	if (putIndex(row)) {
    		Block block = Utils.getLastBlock(rowId, blockSize, tabName);
            
            FutureDHT future = peer.put(Number160.createHash(block.toString())).setData(new Number160(rowId), new Data(row)).start();
            logger.trace("INSERT-PUT-INSERTIONORDER", "BEGIN", Statement.experiment, future.hashCode(), this.hashCode());
            future.addListener(new BaseFutureAdapter<FutureDHT>() {
                @Override
                public void operationComplete(FutureDHT future) throws Exception {
                    if (future.isSuccess()) {
                        logger.debug("INSERT (insertion order): Put succeed!");
                    } else {
                        //add exception?
                        logger.debug("INSERT (insertion order): Put failed!");
                    }
                    done = true;
                    logger.trace("INSERT-PUT-INSERTIONORDER", "END", Statement.experiment, future.hashCode());
                }
            });
    	} else {
			logger.debug("Value already indexed, row was not put!");
		}
        
    }
    
	/**
	 * Insert the row in a free block if possible, otherwise does a putInsertionOrder operation.
	 */
    private void putFullBlocks() throws IOException, MalformedSQLQuery, ClassNotFoundException {
    	Map<Number160, Data> freeBlocks = null;
    	
    	if (freeBlocksHandler.isFullBlocksStorage()) {
    		freeBlocks = freeBlocksHandler.getFreeBlocks();
    	} else {
    		logger.error("FreeBlocksHandler error");
    	}
    	
    	if (freeBlocks.size() > 0) {
    		
    		Number160 blockKey = freeBlocks.keySet().iterator().next();
			@SuppressWarnings("unchecked")
			List<Integer> rowIds = (List<Integer>) freeBlocks.get(blockKey).getObject();
    		
    		int rowId = rowIds.get(0);
    		rowIds.remove(0);
    		logger.debug("FreeBlocks is using this id:" + rowId);
    		if (rowIds.size() > 0) {
    			freeBlocks.put(blockKey, new Data(rowIds));
    		} else {
    			freeBlocks.remove(blockKey);
    		}
    		
    		Row row = getRow(rowId);
    		if (putIndex(row)) {
    			FutureDHT future = peer.put(blockKey).setData(new Number160(rowId), new Data(row)).start();
    			logger.trace("INSERT-PUT-FULLBLOCKS", "BEGIN", Statement.experiment, future.hashCode(), this.hashCode());
                future.addListener(new BaseFutureAdapter<FutureDHT>() {
                    @Override
                    public void operationComplete(FutureDHT future) throws Exception {
                        if (future.isSuccess()) {
                            logger.debug("INSERT (full blocks): Put succeed!");
                        } else {
                            //add exception?
                            logger.debug("INSERT (full blocks): Put failed!");
                        }
                        done = true;
                        logger.trace("INSERT-PUT-FULLBLOCKS", "END", Statement.experiment, future.hashCode());
                    }
                });
    		} else {
    			logger.debug("Value already indexed, row was not put!");
    		}
    	} else {
    		putInsertionOrder();
    	}
    }
    
    /**
     * Insert the value in the indexes, using the operations in IndexHandler class.
     * 
     * @param row
     */
    private boolean putIndex(Row row) throws MalformedSQLQuery, IOException, ClassNotFoundException {
		boolean success = false;
    	if (indexes.size() > 0) {
			for (String index: indexes) {
				try {
					int indexedVal = Integer.parseInt(row.getCol(tc.getColumnId(index)));
					success = indexHandler.get(index).put(row.getRowID(), indexedVal, ti.getDSTRange(), tabName, index, false, this.hashCode());
					ti.setMinMax(index, indexedVal);
				} catch (NumberFormatException e) {
					logger.error("Indexed Column is not an INT", e);
				}
			}
		} else {
			success = true;
		}
		
		if (uniqueIndexes.size() > 0) {
			for (String index: uniqueIndexes) {
				try {
					int indexedVal = Integer.parseInt(row.getCol(tc.getColumnId(index)));
					success = indexHandler.get(index).put(row.getRowID(), indexedVal, ti.getDSTRange(), tabName, index, true, this.hashCode());
					ti.setMinMax(index, indexedVal);
				} catch (NumberFormatException e) {
					logger.error("Indexed Column is not an INT", e);
				}
			}
		} else {
			success = true;
		}
		return success;
	}
    
    /**
     * Generate the new Row.
     * 
     * @param rowId
     * @return row
     */
    private Row getRow(int rowId) throws MalformedSQLQuery {
        Row row = null;
        if (columns == null) {
            if ((values.size() == tc.getNumOfCols())) {
                row = new Row(tabName, rowId, values, tc.getColumns());
            } else {
            	throw new MalformedSQLQuery("Num of Values NOT equal num of Columns!");       
            }
        } 
        else {
            row = new Row(tabName, rowId, tc.getColumns());
            for (int i = 0; i < columns.size(); i++) {
            	row.setCol(columns.get(i), values.get(i));
            }   
        }
        return row;
    }

    public String getTabName() {
		return tabName;
	}
    
    public boolean getDone() {
    	return done;
    }
    
	/**
	 * Empty init from Interface.
	 */
	@Override
	public void init() {
		this.init(null,null,null,null);	
	}

}
