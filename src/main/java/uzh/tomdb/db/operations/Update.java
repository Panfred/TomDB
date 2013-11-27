
package uzh.tomdb.db.operations;

import java.io.IOException;
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
import uzh.tomdb.db.operations.helpers.Block;
import uzh.tomdb.db.operations.helpers.Row;
import uzh.tomdb.db.operations.helpers.SetCondition;
import uzh.tomdb.db.operations.helpers.TempResults;
import uzh.tomdb.db.operations.helpers.Utils;
import uzh.tomdb.db.operations.helpers.WhereCondition;
import uzh.tomdb.p2p.DBPeer;

/**
 *
 * @author Francesco Luminati
 */
public class Update extends Operation implements Operations, TempResults{
	private final Logger logger = LoggerFactory.getLogger(Update.class);
    private String tabName;
    private List<SetCondition> setConditions;
    private List<WhereCondition> whereConditions;
    private String scanType;

    public Update(String tabName, List<SetCondition> setConditions, List<WhereCondition> whereConditions, String scanType) {
        this.tabName = tabName;
        this.tabKey = Number160.createHash(tabName);
        this.setConditions = setConditions;
        this.whereConditions = whereConditions;
        this.scanType = scanType;
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
			
		} catch (ClassNotFoundException | IOException e) {
			logger.error("Data error", e);
		} 
			
    	new Select(tabName, tr, ti, tc, whereConditions, scanType, this).init();
    }

    @Override
	public void addRow(Row row) {
    	if (row.getRowID() > 0) {
			executeUpdate(row);
		} else {
			logger.debug("UPDATE completed");
		}		
	}
    
    private void executeUpdate(Row row) {
    	
    	for (SetCondition cond: setConditions) {
    		row.setCol(cond.getColumn(), cond.getValue());
    	}
    	
    	Data data = null;
		try {
			data = new Data(row);
		} catch (IOException e) {
			logger.error("Data error", e);
		}
    	
    	List<Block> blocks = Utils.getBlocks(row.getRowID(), row.getRowID(), tr.getRowsNum(), tr.getBlockSize(), tabName);
		Number160 blockKey = blocks.get(0).getHash();
		
		FutureDHT future = peer.put(blockKey).setData(new Number160(row.getRowID()), data).start();
		future.addListener(new BaseFutureAdapter<FutureDHT>() {
            @Override
            public void operationComplete(FutureDHT future) throws Exception {
                if (future.isSuccess()) {
                    logger.debug("UPDATE Insert updated Row: Put succeed!");
                } else {
                    //add exception?
                    logger.debug("UPDATE Insert updated Row: Put failed!");
                }
            }
        });
    }
    
    @Override
    public String toString() {
        return "Update{" + "tabName=" + tabName + ", setOperations=" + setConditions + ", whereOperations=" + whereConditions + '}';
    }
    
}
