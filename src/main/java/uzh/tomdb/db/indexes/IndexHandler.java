
package uzh.tomdb.db.indexes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uzh.tomdb.api.Statement;
import uzh.tomdb.db.operations.helpers.Utils;

/**
 * 
 * Handles the indexes and all the related Distributed Segment Tree operations.
 * 
 * @author Francesco Luminati
 *
 */
public class IndexHandler {
	private final Logger logger = LoggerFactory.getLogger(IndexHandler.class);
	private final Peer peer;
	private Map<Integer, IndexedValue> alreadyIndexed = new ConcurrentHashMap<>();
	private int expHash;
	
	public IndexHandler(Peer peer) {
		this.peer = peer;
	}
	
	/**
	 * Operation to insert a new indexed value in the index.
	 * The index is first checked if the value has been already indexed.
	 * 
	 * @param rowId
	 * @param indexedVal
	 * @param upperBound
	 * @param column name
	 */
	public boolean put(int rowId, int indexedVal, int upperBound, String tabName, String tabCol, boolean univocal, int expHash) throws IOException, ClassNotFoundException {
		this.expHash = expHash;
		
		IndexedValue iv = null;
		
		if (alreadyIndexed.containsKey(indexedVal)) {
			iv = alreadyIndexed.get(indexedVal);
		} else {
			iv = checkIndex(indexedVal, upperBound, tabName, tabCol);
		}
		
		if (iv == null) {
			iv = new IndexedValue(indexedVal, rowId);
		} else {
			if (univocal) {
				return false;
			}
			iv.addRowId(rowId);
		}
		alreadyIndexed.put(indexedVal, iv);
		putDST(iv, upperBound, tabName, tabCol);
		return true;
	}
	
	/**
	 * Operation to remove an indexed value from the index.
	 * The value is first identified and then removed. If the rowId list is then empty, it is completely removed from the DST.
	 * 
	 * @param rowId
	 * @param indexedVal
	 * @param upperBound
	 * @param column name
	 */
	public void remove(int rowId, int indexedVal, int upperBound, String tabName, String tabCol, int expHash) throws ClassNotFoundException, IOException {
		this.expHash = expHash;
		
		IndexedValue iv = null;
		List<Integer> rowIds = new ArrayList<>();
		
		iv = checkIndex(indexedVal, upperBound, tabName, tabCol);
		
		if (iv != null) {
			rowIds = iv.getRowIds();
		} else {
			logger.error("Indexed value not found in the DST!");
		}
		
		if (rowIds.size() > 1) {
			rowIds.remove((Object) rowId);
			putDST(iv, upperBound, tabName, tabCol);
		} else if (rowIds.size() == 1) {
			removeDST(iv, upperBound, tabName, tabCol);
		} else {
			logger.error("Indexed value not found in the index!");
		}
	}
	
	/**
	 * Check if the indexed value is already present in the index and returns it.
	 * 
	 * @param indexedVal
	 * @param upperBound
	 * @param column name
	 * @return IndexedValue object
	 */
	private IndexedValue checkIndex(int indexedVal, int upperBound, String tabName, String tabCol) throws ClassNotFoundException, IOException {
		
		Map<Integer, IndexedValue> results = getDSTblocking(indexedVal, indexedVal, upperBound, tabName, tabCol);
		
		if (results.size() > 0) {
			return results.get(indexedVal);
		}
		
		return null;
	}
	
	/**
	 * DHT Operation to put an IndexedValue object on every level of the DST.
	 * 
	 * @param indexedVal
	 * @param upperBound
	 * @param column name
	 */
	private void putDST(IndexedValue indexedVal, int upperBound, String tabName, String tabCol) throws IOException {
			
		  DSTBlock block = new DSTBlock(0, upperBound, tabName, tabCol);
		  
		  final AtomicInteger counter = new AtomicInteger(Utils.getDSTHeight(upperBound));
		  logger.trace("INDEXHANDLER-PUT-DST", "BEGIN", Statement.experiment, expHash, counter.get());
		  
		  for (int i = 0; i <= Utils.getDSTHeight(upperBound); i++) {

		      Number160 key = Number160.createHash(block.toString());
		      FutureDHT future = peer.put(key).setData(new Number160(indexedVal.getIndexedVal()), new Data(indexedVal)).start();
		      future.addListener(new BaseFutureAdapter<FutureDHT>() {
		              @Override
		              public void operationComplete(FutureDHT future) throws Exception {
		                  if (future.isSuccess()) {
		                      logger.debug("INSERT DST: Put succeed!");
		                  } else {
		                      //add exception?
		                      logger.debug("INSERT DST: Put failed!");
		                  }
		                  if (counter.decrementAndGet() == 0) {
		                	  logger.trace("INDEXHANDLER-PUT-DST", "END", Statement.experiment, expHash);
		                  }
		              }
		          });
		      block = block.split(indexedVal.getIndexedVal());
		  }
	 
	}
	
	/**
	 * DHT Operation to remove an IndexedValue object from every level of the DST.
	 * 
	 * @param indexedVal
	 * @param upperBound
	 * @param column name
	 */
	private void removeDST(IndexedValue iv, int upperBound, String tabName, String tabCol) {
		
		DSTBlock block = new DSTBlock(0, upperBound, tabName, tabCol);
		
		final AtomicInteger counter = new AtomicInteger(Utils.getDSTHeight(upperBound));
		logger.trace("INDEXHANDLER-REMOVE-DST", "BEGIN", Statement.experiment, expHash, counter.get());
		  
		for (int i = 0; i <= Utils.getDSTHeight(upperBound); i++) {
			FutureDHT future = peer.remove(block.getHash()).setContentKey(new Number160(iv.getIndexedVal())).start();
			future.addListener(new BaseFutureAdapter<FutureDHT>() {
	              @Override
	              public void operationComplete(FutureDHT future) throws Exception {
	                  if (future.isSuccess()) {
	                      logger.debug("REMOVE DST: Remove succeed!");
	                  } else {
	                      //add exception?
	                      logger.debug("REMOVE DST: Remove failed!");
	                  }
	                  if (counter.decrementAndGet() == 0) {
	                	  logger.trace("INDEXHANDLER-REMOVE-DST", "END", Statement.experiment, expHash);
	                  }
	              }
	          });
	      block = block.split(iv.getIndexedVal());
		}
		
	}
	
	/**
	 * Blocking DHT operation to get the IndexedValue from the index.
	 * The blocking is reached with the wait/notify construct.
	 * 
	 * @param from
	 * @param to
	 * @param upperBound
	 * @param column
	 */
	private Map<Integer, IndexedValue> getDSTblocking(int from, int to, int upperBound, String tabName, String tabCol) throws ClassNotFoundException, IOException {
      List<DSTBlock> rowsBlocks = Utils.splitRange(from, to, upperBound, tabName, tabCol);
      Map<Integer, IndexedValue> results = new HashMap<>();
      AtomicInteger counter = new AtomicInteger(0);
      
      logger.trace("INDEXHANDLER-GET-DST", "BEGIN", Statement.experiment, expHash, rowsBlocks.size());
      
      getDST(rowsBlocks, upperBound, new HashSet<String>(), results, counter, this); 
      
      while (counter.get() > 0) {
  	    synchronized (this) { 
  	        try {
					this.wait();
				} catch (InterruptedException e) {
					logger.error("Wait interrupted", e);
				}
  	    }
      }
      
      logger.trace("INDEXHANDLER-GET-DST", "END", Statement.experiment, expHash);
      
      return results;
  }
  
  /**
   * Recursive method for searching all items for a range. We need to do recursion because when we find out that a
   * node is full, we need to go further.
   * 
   * @param inters
   *            The interval to search for
   * @param results
   *            The list where the results are stored
   * @param already
   *            Already queried intervals
   * @throws ClassNotFoundException .
   * @throws IOException .
   */
	private void getDST(List<DSTBlock> blocks, final int upperBound,
			final Set<String> already,
			final Map<Integer, IndexedValue> results,
			final AtomicInteger counter, final IndexHandler ih)
			throws ClassNotFoundException, IOException {

		for (final DSTBlock block: blocks) {

			// we don't query the same thing again
			if (already.contains(block.toString())) {
				continue;
			}

			Number160 key = block.getHash();
			already.add(block.toString());

			// get the interval
			FutureDHT future = peer.get(key).setAll().start();
			counter.incrementAndGet();
			future.addListener(new BaseFutureAdapter<FutureDHT>() {
				@Override
				public void operationComplete(FutureDHT future) throws Exception {
					
					if (future.isSuccess()) {
						logger.debug("GET DST: Get succeed!");

						Map<Number160, Data> map = future.getDataMap();
						Set<Number160> keys = map.keySet();

						for (Number160 entry : keys) {
							IndexedValue iv = (IndexedValue) map.get(entry)
									.getObject();
							if (!results.containsKey(iv.getIndexedVal())) {
								results.put(iv.getIndexedVal(), iv);
							}
						}

						// IF block is full, we need to get children
						if (map.size() == upperBound) {
							logger.debug(block + " FULL!");
							logger.trace("INDEXHANDLER-GET-DST", "RECURSION", Statement.experiment, expHash, 2);
							getDST(block.split(), upperBound, already, results,
									counter, ih);
						}
						
					} else {
						// add exception?
						logger.debug("GET DST: Get failed!");
					}
					if (counter.decrementAndGet() == 0) {
						synchronized (ih) {
							ih.notify();
						}
					}
				}	
			});
		}

	}

}
