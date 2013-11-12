package uzh.tomdb.db.operations.helpers;

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
	private boolean fullBlocksStorage = false;
	private Map<Number160, Data> freeBlocks;
	
	public FreeBlocksHandler(String tabName) {
		key = Number160.createHash("FreeBlocks:"+tabName);
		this.tabName = tabName;
		init();
	}
	
	private void init() {
		checkFullBlocksStorage();
		fetch();
	}
	
	private void fetch() {
		if(fullBlocksStorage) {
			FutureDHT future = peer.get(key).setAll().start();
	        future.addListener(new BaseFutureAdapter<FutureDHT>() {
	            @Override
	            public void operationComplete(FutureDHT future) throws Exception {
	                if (future.isSuccess()) {
	                	freeBlocks = future.getDataMap();
	                    logger.debug("FREEBLOCKS: Get succeed!");     
	                } else {
	                    //add exception?
	                    freeBlocks = new HashMap<>();
	                    logger.debug("FREEBLOCKS: Get failed!");
	                }
	            }
	        });
		}
	}

	public void update() {
		if(fullBlocksStorage && freeBlocks.size() > 0) { 
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
	
	private void checkFullBlocksStorage() {
		try {
			TableRows tr = (TableRows) DBPeer.getTabRows().get(Number160.createHash(tabName)).getObject();
			if (tr.getStorage().equals("fullblocks")) {
				fullBlocksStorage = true;
			}
		} catch (ClassNotFoundException | IOException e) {
			logger.error("Data error", e);
		}
	}

	public boolean isFullBlocksStorage() {
		return fullBlocksStorage;
	}

	public Map<Number160, Data> getFreeBlocks() {
		return freeBlocks;
	}
	
}
