
package uzh.tomdb.db.indexes;

import java.io.IOException;

import net.tomp2p.p2p.Peer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Handles the univocal indexes extending the IndexHandler class.
 * 
 * @author Francesco Luminati
 *
 */
public class UniqueIndexHandler extends IndexHandler{
	private final Logger logger = LoggerFactory.getLogger(UniqueIndexHandler.class);
	
	public UniqueIndexHandler(Peer peer, int expHash) {
		super(peer, expHash);
	}
	
	/**
	 * Operation to insert a new univocal indexed value in the index.
	 * The index is first checked if the value has been already indexed, in that case an error is thrown.
	 * 
	 * @param rowId
	 * @param indexedVal
	 * @param upperBound
	 * @param column name
	 */
	@Override
	public boolean put(int rowId, int indexedVal, int upperBound, String tabCol) throws ClassNotFoundException, IOException {
		
		IndexedValue iv = null;
		
		iv = checkIndex(indexedVal, upperBound, tabCol);
		
		if (iv == null) {
			iv = new IndexedValue(indexedVal, rowId);
			putDST(iv, upperBound, tabCol);
			return true;
		} else {
			logger.error("The value was already indexed!!");
			return false;
		}
		
	}

}
