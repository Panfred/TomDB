package uzh.tomdb.db.indexes;

import java.io.IOException;

import net.tomp2p.p2p.Peer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UniqueIndexHandler extends IndexHandler{
	private final Logger logger = LoggerFactory.getLogger(UniqueIndexHandler.class);
	
	public UniqueIndexHandler(Peer peer) {
		super(peer);
	}
	
	@Override
	public boolean put(int rowId, int indexedVal, int upperBound, String column) throws ClassNotFoundException, IOException {
		
		IndexedValue iv = null;
		
		iv = checkIndex(indexedVal, upperBound, column);
		
		if (iv == null) {
			iv = new IndexedValue(indexedVal, rowId);
			putDST(iv, upperBound, column);
			return true;
		} else {
			logger.error("The value was already indexed!!");
			return false;
		}
		
	}

}
