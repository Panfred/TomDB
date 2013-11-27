
package uzh.tomdb.db.operations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;
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
 * @author Francesco Luminati
 */
public class Delete extends Operation implements Operations, TempResults{
	private final Logger logger = LoggerFactory.getLogger(Delete.class);
    private List<WhereCondition> whereConditions;
    private String scanType;
    private FreeBlocksHandler freeBlocksHandler;
    private Map<Number160, Data> freeBlocks;
    private IndexHandler ih;
    private Data data;
    
    
    public Delete(String tabName, List<WhereCondition> whereOperations, String scanType) {
        super();
    	super.tabName = tabName;
    	this.scanType = scanType;
        this.whereConditions = whereOperations;
        super.tabKey = Number160.createHash(tabName);
        freeBlocksHandler = new FreeBlocksHandler(tabName);
        ih = new IndexHandler(peer);
    }

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
			
    	new Select(tabName, tr, ti, tc, whereConditions, scanType, this).init();
		
		//should be blocking?
		freeBlocks = freeBlocksHandler.getFreeBlocks();
		
    }
    
    @Override
	public void addRow(Row row) {
		if (row.getRowID() >= 0) {
			executeDelete(row);
		} else {
			freeBlocksHandler.update();
			logger.debug("DELETE completed");
		}
	}
	
	private void executeDelete(Row row) { 
		
		List<Block> blocks = Utils.getBlocks(row.getRowID(), row.getRowID(), tr.getRowsNum(), tr.getBlockSize(), tabName);
		Number160 blockKey = blocks.get(0).getHash();
		
		FutureDHT future = peer.put(blockKey).setData(new Number160(row.getRowID()), data).start();
		future.addListener(new BaseFutureAdapter<FutureDHT>() {
            @Override
            public void operationComplete(FutureDHT future) throws Exception {
                if (future.isSuccess()) {
                    logger.debug("DELETE Insert empty Row: Put succeed!");
                } else {
                    //add exception?
                    logger.debug("DELETE Insert empty Row: Put failed!");
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
	
	private void updateIndexes(Row row) throws ClassNotFoundException, IOException {
		for (String col: ti.getIndexes()) {
			try {
				int indexedVal = Integer.parseInt(row.getCol(col));
				ih.remove(row.getRowID(), indexedVal, ti.getDSTRange(), col);
				//TODO SET index Min Max ???
			} catch (NumberFormatException e) {
				logger.error("Indexed Column is not an INT", e);
			}
		}
		for (String col: ti.getUniqueIndexes()) {
			try {
				int indexedVal = Integer.parseInt(row.getCol(col));
				ih.remove(row.getRowID(), indexedVal, ti.getDSTRange(), col);
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
