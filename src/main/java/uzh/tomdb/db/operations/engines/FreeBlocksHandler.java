
package uzh.tomdb.db.operations.engines;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;
import uzh.tomdb.api.Statement;
import uzh.tomdb.db.TableRows;
import uzh.tomdb.p2p.DBPeer;

/**
 * 
 * Handler for the Free Blocks in case of the fullblocks storage method.
 * 
 * @author Francesco Luminati
 */
public class FreeBlocksHandler {
	private final Logger logger = LoggerFactory.getLogger(FreeBlocksHandler.class);
	private final Peer peer = DBPeer.getPeer();
	private String tabName;
	private Number160 key;
	/**
	 * Map containing the block key and a list of free row IDs.
	 */
	private Map<Number160, Data> freeBlocks = new HashMap<>();
	private int isFullBlocksStorage = 0;
	
	public FreeBlocksHandler(String tabName) {
		key = Number160.createHash("FreeBlocks:"+tabName);
		this.tabName = tabName;
		init();
	}
	
	/**
	 * Fetch the free blocks only if the storage is a fullblocks storage.
	 */
	private void init() {
		if (isFullBlocksStorage()) {
			fetch();
		}
	}
	
	/**
	 * Get the free blocks from the DHT for the given table.
	 */
	private void fetch() {
			FutureDHT future = peer.get(key).setAll().start();
			logger.trace("FREEBLOCKSHANDLER-FETCH", "BEGIN", Statement.experiment, tabName, future.hashCode());
			future.awaitUninterruptibly();
            if (future.isSuccess()) {
            	freeBlocks = future.getDataMap();
                logger.debug("FREEBLOCKS: Get succeed!");     
            } else {
                //add exception?
                logger.debug("FREEBLOCKS: Get failed!");
            }
            logger.trace("FREEBLOCKSHANDLER-FETCH", "END", Statement.experiment, tabName, future.hashCode());
	}
	
	/**
	 * Update the free blocks on the DHT for the given table.
	 */
	public void update() {
		if(isFullBlocksStorage() && freeBlocks.size() > 0) { 
			FutureDHT future = peer.put(key).setDataMap(freeBlocks).start();
			logger.trace("FREEBLOCKSHANDLER-UPDATE", "BEGIN", Statement.experiment, tabName, future.hashCode());
	        future.addListener(new BaseFutureAdapter<FutureDHT>() {
	            @Override
	            public void operationComplete(FutureDHT future) throws Exception {
	                if (future.isSuccess()) {
	                    logger.debug("FREEBLOCKS: Put succeed!");
	                } else {
	                    //add exception?
	                    logger.debug("FREEBLOCKS: Put failed!");
	                }
	                logger.trace("FREEBLOCKSHANDLER-UPDATE", "END", Statement.experiment, tabName, future.hashCode());
	            }
	        });
		}
	}

	/**
	 * Check if the table was set as fullblocks storage.
	 */
	public boolean isFullBlocksStorage() {
		if (isFullBlocksStorage == 0) {
			try {
				TableRows tr = (TableRows) DBPeer.getTabRows().get(Number160.createHash(tabName)).getObject();
				if (tr.getStorage().equals("fullblocks")) {
					isFullBlocksStorage = 1;
					return true;
				} else {
					isFullBlocksStorage = 2;
					return false;
				}
			} catch (ClassNotFoundException | IOException e) {
				logger.error("Data error", e);
			}
		} else {
			if (isFullBlocksStorage == 1) {
				return true;
			} else {
				return false;
			}
		}
		return false;
	}

	public Map<Number160, Data> getFreeBlocks() {
		return freeBlocks;
	}
	
}
