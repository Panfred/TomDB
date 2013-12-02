
package uzh.tomdb.db.operations.engines;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uzh.tomdb.api.Statement;
import uzh.tomdb.db.TableIndexes;
import uzh.tomdb.db.TableRows;
import uzh.tomdb.db.operations.CreateTable;
import uzh.tomdb.db.operations.Delete;
import uzh.tomdb.db.operations.Insert;
import uzh.tomdb.db.operations.Operations;
import uzh.tomdb.db.operations.Update;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import uzh.tomdb.p2p.DBPeer;

/**
 *
 * The QueryEngine is used to start the buffered query objects of CREATE TABLE, INSERT, UPDATE and DELETE queries.
 *
 * @author Francesco Luminati
 */
public class QueryEngine {
    private final Logger logger = LoggerFactory.getLogger(QueryEngine.class);
    private List<Operations> bufferedQueries;
    private List<CreateTable> creates;
    private List<Insert> inserts;
    private List<Update> updates;
    private List<Delete> deletes;
    
    public QueryEngine(List<Operations> bufferedQueries) {
        this.bufferedQueries = bufferedQueries;
    }
    
    /**
     * Internal parser to separate the buffered objects in the respective query type list and start the operations.
     */
    public void start() {
        
        for (Object obj: bufferedQueries) {
            
            switch(obj.getClass().getName()) {
                case "uzh.tomdb.db.operations.Insert":
                    if (inserts == null) {
                        inserts = new ArrayList<>();
                    }
                    inserts.add((Insert) obj);
                    break;
                case "uzh.tomdb.db.operations.CreateTable":
                    if (creates == null) {
                        creates = new ArrayList<>();
                    }
                    creates.add((CreateTable) obj);
                    break;
                case "uzh.tomdb.db.operations.Update":
                    if (updates == null) {
                        updates = new ArrayList<>();
                    }
                    updates.add((Update) obj);
                    break;
                case "uzh.tomdb.db.operations.Delete":
                    if (deletes == null) {
                        deletes = new ArrayList<>();
                    }
                    deletes.add((Delete) obj);
                    break;
            }
        }
        
        if (inserts != null) {
            inserts();
        }
        if (creates != null) {
            creates();
        }
        if (updates != null) {
            updates();
        }
        if (deletes != null) {
            deletes();
        }
    }

	/**
     * Execute INSERT queries.
     * Actualize MetaData before and after the execution.
     * For better performances, the table MetaData and FreeBlocksHandler are not actualized for every queries, but just at the end or when the insert happens in a different table.
     */
    private void inserts() {
        DBPeer.fetchTableRows();
        DBPeer.fetchTableIndexes();
        
        Map<Number160, Data> tabRows = DBPeer.getTabRows();
		Map<Number160, Data> tabIndexes = DBPeer.getTabIndexes();
		
        String tabName = null;
        Number160 tabKey = null;
        FreeBlocksHandler freeBlocks = null;
        TableRows tr = null;
        TableIndexes ti = null;

        for (Insert ins: inserts) {
            logger.trace("INSERT BEGIN", ins.hashCode(), Statement.experiment);
            if (tabName == null) {
            	tabName = ins.getTabName();
            	tabKey = Number160.createHash(tabName);
            	freeBlocks = new FreeBlocksHandler(tabName);
            	try {
    				tr = (TableRows) tabRows.get(tabKey).getObject();
    				ti =  (TableIndexes) tabIndexes.get(tabKey).getObject();
    			} catch (ClassNotFoundException | IOException e) {
    				logger.error("Data error", e);
    			}
            } 
            if (!tabName.equals(ins.getTabName())) {
    			try {
    				tabRows.put(tabKey, new Data(tr));
        			tabIndexes.put(tabKey, new Data(ti));
        			tabName = ins.getTabName();
                	tabKey = Number160.createHash(tabName);
    				tr = (TableRows) tabRows.get(tabKey).getObject();
    				ti =  (TableIndexes) tabIndexes.get(tabKey).getObject();
    			} catch (ClassNotFoundException | IOException e) {
    				logger.error("Data error", e);
    			}
            } 
            if (!tabName.equals(ins.getTabName()) && freeBlocks.isFullBlocksStorage()) {
            	freeBlocks.update();
            	freeBlocks = new FreeBlocksHandler(tabName);
            }
            
            ins.init(freeBlocks, tr, ti);

            logger.trace("INSERT END", ins.hashCode(), Statement.experiment);
        }
        
        if (freeBlocks.isFullBlocksStorage()) {
        	freeBlocks.update();
        }
       
        try {
			tabRows.put(tabKey, new Data(tr));
			tabIndexes.put(tabKey, new Data(ti));
		} catch (IOException e) {
			logger.error("Data error", e);
		}
        
        DBPeer.updateTableRows();
        DBPeer.updateTableIndexes();
    }
    
	/**
     * Execute CREATE TABLE queries.
     * Actualize MetaData before and after the execution.
     */
    private void creates() {
    	DBPeer.fetchTableColumns();
    	DBPeer.fetchTableRows();
    	DBPeer.fetchTableIndexes();
    	
        for (int i = 0; i < creates.size(); i++) {
            creates.get(i).init();
        }
        
        DBPeer.updateTableColumns();
        DBPeer.updateTableRows();
        DBPeer.updateTableIndexes();
    }
    
    /**
     * Execute UPDATE queries.
     * Actualize MetaData before the execution.
     */
    private void updates() {
    	DBPeer.fetchTableRows();
        DBPeer.fetchTableIndexes();
        
        for (Update update: updates) {
        	update.init();
        }
    }
    
    /**
     * Execute DELETE queries.
     * Actualize MetaData before the execution.
     */
    private void deletes() {
    	DBPeer.fetchTableRows();
        DBPeer.fetchTableIndexes();
        
        for (Delete delete: deletes) {
        	delete.init();
        }
        
        //DBPeer.updateTableIndexes(); //Set min max of indexes not done
    }
 
}
