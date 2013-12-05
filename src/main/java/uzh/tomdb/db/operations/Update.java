
package uzh.tomdb.db.operations;

import java.io.IOException;
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
import uzh.tomdb.db.operations.helpers.Block;
import uzh.tomdb.db.operations.helpers.Row;
import uzh.tomdb.db.operations.helpers.SetCondition;
import uzh.tomdb.db.operations.helpers.TempResults;
import uzh.tomdb.db.operations.helpers.Utils;
import uzh.tomdb.db.operations.helpers.WhereCondition;
import uzh.tomdb.p2p.DBPeer;

/**
*
* UPDATE SQL operation.
*
* @author Francesco Luminati
*/
public class Update extends Operation implements Operations, TempResults{
	private final Logger logger = LoggerFactory.getLogger(Update.class);
	/**
	 * Set conditions to update the values of the given columns.
	 */
    private List<SetCondition> setConditions;
    /**
     * Where conditions for the SELECT operation.
     */
    private List<WhereCondition> whereConditions;
    /**
     * Scan type (tablescan/indexscan) defined in the OPTIONS statement.
     */
    private String scanType;
    private AtomicInteger futureHandler = new AtomicInteger(0);

    public Update(String tabName, List<SetCondition> setConditions, List<WhereCondition> whereConditions, String scanType) {
    	super();
        super.tabName = tabName;
        super.tabKey = Number160.createHash(tabName);
        this.setConditions = setConditions;
        this.whereConditions = whereConditions;
        this.scanType = scanType;
    }
    
    /**
     * Initializes the table MetaData and execute a SELECT to identify the rows IDs that are going to be updated.
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
			
		} catch (ClassNotFoundException | IOException e) {
			logger.error("Data error", e);
		} 
			
		logger.trace("UPDATE-WHOLE", "BEGIN", Statement.experiment, this.hashCode());
		
    	new Select(tabName, tr, ti, tc, whereConditions, scanType, this).init();
    }

    /**
     * Gets the rows from the SELECT operation.
     * 
     * @param row
     */
    @Override
	public void addRow(Row row) {
    	if (row.getRowID() > 0) {
			executeUpdate(row);
		}		
	}
    
    /**
     * Set the new values for the given columns and put the row in the table DTH.
     * 
     * @param row
     */
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
		futureHandler.incrementAndGet();
		logger.trace("UPDATE-PUT-ROW", "BEGIN", Statement.experiment, future.hashCode(), this.hashCode());
		future.addListener(new BaseFutureAdapter<FutureDHT>() {
            @Override
            public void operationComplete(FutureDHT future) throws Exception {
                if (future.isSuccess()) {
                    logger.debug("UPDATE Insert updated Row: Put succeed!");
                } else {
                    //add exception?
                    logger.debug("UPDATE Insert updated Row: Put failed!");
                }
                logger.trace("UPDATE-PUT-ROW", "END", Statement.experiment, future.hashCode());
                if (futureHandler.decrementAndGet() == 0) {
                	logger.debug("UPDATE completed");
        			logger.trace("UPDATE-WHOLE", "END", Statement.experiment, this.hashCode());
                }
            }
        });
    }
    
    @Override
    public String toString() {
        return "Update{" + "tabName=" + tabName + ", setOperations=" + setConditions + ", whereOperations=" + whereConditions + '}';
    }
    
}
