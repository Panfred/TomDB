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
import uzh.tomdb.db.TableRows;
import uzh.tomdb.p2p.DBPeer;

public class FreeBlocksHandler {
	private final Logger logger = LoggerFactory.getLogger(FreeBlocksHandler.class);
	private final Peer peer = DBPeer.getPeer();
	private String tabName;
	private Number160 key;
	private Map<Number160, Data> freeBlocks;
	private int isFullBlocksStorage = 0;
	
	public FreeBlocksHandler(String tabName) {
		key = Number160.createHash("FreeBlocks:"+tabName);
		this.tabName = tabName;
		init();
	}
	
	private void init() {
		if (isFullBlocksStorage()) {
			fetch();
		}
	}
	
	//Blocking
	private void fetch() {
			FutureDHT future = peer.get(key).setAll().start();
			future.awaitUninterruptibly();
            if (future.isSuccess()) {
            	freeBlocks = future.getDataMap();
                logger.debug("FREEBLOCKS: Get succeed!");     
            } else {
                //add exception?
                freeBlocks = new HashMap<>();
                logger.debug("FREEBLOCKS: Get failed!");
            }
	}

	public void update() {
		if(isFullBlocksStorage() && freeBlocks.size() > 0) { 
			FutureDHT future = peer.put(key).setDataMap(freeBlocks).start();
	        future.addListener(new BaseFutureAdapter<FutureDHT>() {
	            @Override
	            public void operationComplete(FutureDHT future) throws Exception {
	                if (future.isSuccess()) {
	                    logger.debug("FREEBLOCKS: Put succeed!");
	                } else {
	                    //add exception?
	                    logger.debug("FREEBLOCKS: Put failed!");
	                }
	            }
	        });
		}
	}


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
