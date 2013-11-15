
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

import uzh.tomdb.db.TableColumns;
import uzh.tomdb.db.TableIndexes;
import uzh.tomdb.db.TableRows;
import uzh.tomdb.db.indexes.IndexHandler;
import uzh.tomdb.db.indexes.UniqueIndexHandler;
import uzh.tomdb.db.operations.engines.FreeBlocksHandler;
import uzh.tomdb.db.operations.helpers.Block;
import uzh.tomdb.db.operations.helpers.Row;
import uzh.tomdb.db.operations.helpers.Utils;
import uzh.tomdb.p2p.DBPeer;
import uzh.tomdb.parser.MalformedSQLQuery;



/**
 *
 * @author Francesco Luminati
 */
public class Insert extends Operation implements Operations{
    private final Logger logger = LoggerFactory.getLogger(Insert.class);
    private List<String> columns;
    private List<String> values;
	private FreeBlocksHandler freeBlocksHandler;
    private List<String> indexes;
    private List<String> uniqueIndexes;
    
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
    
	public void init(FreeBlocksHandler freeBlocksHandler, TableRows tr, TableIndexes ti) {
		this.freeBlocksHandler = freeBlocksHandler;
		Map<Number160, Data> tabColumns = DBPeer.getTabColumns();
		
		this.tr = tr;
		this.ti = ti;
		
		try {
			tc = (TableColumns) tabColumns.get(tabKey).getObject();
			indexes = ti.getIndexes();
			uniqueIndexes = ti.getUniqueIndexes();

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

	private void putInsertionOrder() throws IOException, MalformedSQLQuery, ClassNotFoundException {

    	int rowId = tr.incrementAndGetRowsNum();
    	int blockSize = tr.getBlockSize();
    	
    	Row row = getRow(rowId);
    	
        Block block = Utils.getLastBlock(rowId, blockSize, tabName);

        FutureDHT future = peer.put(Number160.createHash(block.toString())).setData(new Number160(rowId), new Data(row)).start();
        future.addListener(new BaseFutureAdapter<FutureDHT>() {
            @Override
            public void operationComplete(FutureDHT future) throws Exception {
                if (future.isSuccess()) {
                    logger.debug("INSERT (insertion order): Put succeed!");
                } else {
                    //add exception?
                    logger.debug("INSERT (insertion order): Put failed!");
                }
            }
        });
        putIndex(row);
    }
    
    private void putFullBlocks() throws IOException, MalformedSQLQuery, ClassNotFoundException {
    	Map<Number160, Data> freeBlocks = null;
    	
    	if (freeBlocksHandler.isFullBlocksStorage()) {
    		freeBlocks = freeBlocksHandler.getFreeBlocks();
    	} else {
    		logger.error("FreeBlocksHandler error");
    	}
    	
    	if (freeBlocks.size() > 0) {
    		Number160 blockKey = freeBlocks.keySet().iterator().next();
    		int rowId = (int) freeBlocks.get(blockKey).getObject();
    		
    		Row row = getRow(rowId);

            FutureDHT future = peer.put(blockKey).setData(new Number160(rowId), new Data(row)).start();
            future.addListener(new BaseFutureAdapter<FutureDHT>() {
                @Override
                public void operationComplete(FutureDHT future) throws Exception {
                    if (future.isSuccess()) {
                        logger.debug("INSERT (full blocks): Put succeed!");
                    } else {
                        //add exception?
                        logger.debug("INSERT (full blocks): Put failed!");
                    }
                }
            });
            
            freeBlocks.remove(blockKey);
            putIndex(row);
    		
    	} else {
    		putInsertionOrder();
    	}
    }
    
    private void putIndex(Row row) throws MalformedSQLQuery, IOException, ClassNotFoundException {
		
    	if (indexes.size() > 0) {
			IndexHandler ih = new IndexHandler(peer);
			for (String index: indexes) {
				try {
					int indexedVal = Integer.parseInt(row.getCol(tc.getColumnId(index)));
					ih.put(row.getRowID(), indexedVal, ti.getDSTRange(), index);
					ti.setMinMax(index, indexedVal);
				} catch (NumberFormatException e) {
					logger.error("Indexed Column is not an INT", e);
				}
			}
		}
		
		if (uniqueIndexes.size() > 0) {
			UniqueIndexHandler ih = new UniqueIndexHandler(peer);
			for (String index: uniqueIndexes) {
				try {
					int indexedVal = Integer.parseInt(row.getCol(tc.getColumnId(index)));
					ih.put(row.getRowID(), indexedVal, ti.getDSTRange(), index);
					ti.setMinMax(index, indexedVal);
				} catch (NumberFormatException e) {
					logger.error("Indexed Column is not an INT", e);
				}
			}
		}
		
	}

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
    
	/**
	 * Empty init from Interface.
	 */
	@Override
	public void init() {
		this.init(null,null,null);	
	}

}
