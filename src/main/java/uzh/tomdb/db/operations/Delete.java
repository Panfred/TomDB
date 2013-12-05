
package uzh.tomdb.db.operations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;
import uzh.tomdb.api.Statement;
import uzh.tomdb.db.TableColumns;
import uzh.tomdb.db.TableIndexes;
import uzh.tomdb.db.TableRows;
import uzh.tomdb.db.indexes.IndexHandler;
import uzh.tomdb.db.operations.engines.FreeBlocksHandler;
import uzh.tomdb.db.operations.helpers.Block;
import uzh.tomdb.db.operations.helpers.Row;
import uzh.tomdb.db.operations.helpers.TempResults;
import uzh.tomdb.db.operations.helpers.Utils;
import uzh.tomdb.db.operations.helpers.WhereCondition;
import uzh.tomdb.p2p.DBPeer;

/**
*
* DELETE SQL operation.
*
* @author Francesco Luminati
*/
public class Delete extends Operation implements Operations, TempResults{
	private final Logger logger = LoggerFactory.getLogger(Delete.class);
	/**
	 * Where conditions for the SELECT operation.
	 */
    private List<WhereCondition> whereConditions;
    /**
     * Scan type (tablescan/indexscan) defined in the OPTIONS statement.
     */
    private String scanType;
    private FreeBlocksHandler freeBlocksHandler;
    private Map<Number160, Data> freeBlocks;
    private IndexHandler ih;
    private Data data;
    private AtomicInteger futureHandler = new AtomicInteger(0);
    
    public Delete(String tabName, List<WhereCondition> whereOperations, String scanType) {
        super();
    	super.tabName = tabName;
    	super.tabKey = Number160.createHash(tabName);
    	this.scanType = scanType;
        this.whereConditions = whereOperations;
        
        freeBlocksHandler = new FreeBlocksHandler(tabName);
        ih = new IndexHandler(peer, this.hashCode());
    }
    
    /**
     * Initializes the table MetaData, creates an empty Row and execute a SELECT to identify the rows IDs that are going to be deleted.
     * The SELECT operation gets this object and pushes the rows back to this object.
     */
    @Override
    public void init() {
    	Map<Number160, Data> tabColumns = DBPeer.getTabColumns();
		Map<Number160, Data> tabRows = DBPeer.getTabRows();
		Map<Number160, Data> tabIndexes = DBPeer.getTabIndexes();

		try {
			tc = (TableColumns) tabColumns.get(tabKey).getObject();
			tr = (TableRows) tabRows.get(tabKey).getObject();
			ti = (TableIndexes) tabIndexes.get(tabKey).getObject();
			
			data = new Data(new Row(-1));
		} catch (ClassNotFoundException | IOException e) {
			logger.error("Data error", e);
		} 
			
		logger.trace("DELETE-WHOLE", "BEGIN", Statement.experiment, this.hashCode());
		
    	new Select(tabName, tr, ti, tc, whereConditions, scanType, this).init();
		
		//should be blocking?
		freeBlocks = freeBlocksHandler.getFreeBlocks();
		
    }
    
    /**
     * Gets the rows from the SELECT operation. 
     * When the SELECT is done, the free blocks entry in the DHT is updated.
     * 
     * @param row
     */
    @Override
	public void addRow(Row row) {
		if (row.getRowID() >= 0) {
			executeDelete(row);
		}
	}
	
    /**
     * Puts an empty row in the DHT table for the given row ID and executes the update for free blocks and indexes.
     * 
     * @param row
     */
	private void executeDelete(Row row) { 
		
		List<Block> blocks = Utils.getBlocks(row.getRowID(), row.getRowID(), tr.getRowsNum(), tr.getBlockSize(), tabName);
		Number160 blockKey = blocks.get(0).getHash();
		
		FutureDHT future = peer.put(blockKey).setData(new Number160(row.getRowID()), data).start();
		futureHandler.incrementAndGet();
		logger.trace("DELETE-PUT-ROW", "BEGIN", Statement.experiment, future.hashCode(), this.hashCode());
		future.addListener(new BaseFutureAdapter<FutureDHT>() {
            @Override
            public void operationComplete(FutureDHT future) throws Exception {
                if (future.isSuccess()) {
                    logger.debug("DELETE Insert empty Row: Put succeed!");
                } else {
                    //add exception?
                    logger.debug("DELETE Insert empty Row: Put failed!");
                }
                logger.trace("DELETE-PUT-ROW", "END", Statement.experiment, future.hashCode());
                if (futureHandler.decrementAndGet() == 0) {
                	freeBlocksHandler.update();
                	logger.debug("DELETE completed");
        			logger.trace("DELETE-WHOLE", "END", Statement.experiment, this.hashCode());
                }
            }
        });
		
		try {
			addToFreeBlocks(row, blockKey);
			updateIndexes(row);
		} catch (ClassNotFoundException | IOException e) {
			logger.error("Data error", e);
		} 
		
	}
	
	/**
	 * If the storage type is on freeblocks, the deleted row ID is added to the free blocks list for the given block.
	 * 
	 * @param row
	 * @param blockKey
	 */
	@SuppressWarnings("unchecked")
	private void addToFreeBlocks(Row row, Number160 blockKey) throws ClassNotFoundException, IOException {
		if (freeBlocksHandler.isFullBlocksStorage()) {
			List<Integer> rowIds;
			if (freeBlocks.containsKey(blockKey)) {
				rowIds = (List<Integer>) freeBlocks.get(blockKey).getObject();
			} else {
				rowIds = new ArrayList<>();
			}
			rowIds.add(row.getRowID());
			freeBlocks.put(blockKey, new Data(rowIds));
		}
	}
	
	/**
	 * The row is removed from the indexes, using the operations in IndexHandler class.
	 * 
	 * @param row
	 */
	private void updateIndexes(Row row) throws ClassNotFoundException, IOException {
		for (String col: ti.getIndexes()) {
			try {
				int indexedVal = Integer.parseInt(row.getCol(col));
				ih.remove(row.getRowID(), indexedVal, ti.getDSTRange(), tabName+":"+col);
				//TODO SET index Min Max ???
			} catch (NumberFormatException e) {
				logger.error("Indexed Column is not an INT", e);
			}
		}
		for (String col: ti.getUnivocalIndexes()) {
			try {
				int indexedVal = Integer.parseInt(row.getCol(col));
				ih.remove(row.getRowID(), indexedVal, ti.getDSTRange(), tabName+":"+col);
				//TODO SET index Min Max ???
			} catch (NumberFormatException e) {
				logger.error("Indexed Column is not an INT", e);
			}
		}
	}

	@Override
    public String toString() {
        return "Delete{" + "tabName=" + tabName + ", whereOperations=" + whereConditions + '}';
    }

  
}
