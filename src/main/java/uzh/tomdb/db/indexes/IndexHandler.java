
package uzh.tomdb.db.indexes;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uzh.tomdb.db.operations.helpers.Utils;

/**
 * 
 * @author Francesco Luminati
 *
 */
public class IndexHandler {
	private final Logger logger = LoggerFactory.getLogger(IndexHandler.class);
	private final Peer peer;
	
	public IndexHandler(Peer peer) {
		this.peer = peer;
	}
	
	public boolean put(int rowId, int indexedVal, int upperBound, String column) throws IOException, ClassNotFoundException {
		IndexedValue iv = null;
		iv = checkIndex(indexedVal, upperBound, column);
		
		if (iv == null) {
			iv = new IndexedValue(indexedVal, rowId);
		} else {
			iv.addRowId(rowId);
		}
		putDST(iv, upperBound, column);
		return true;
	}
	
	public void remove(int rowId, int indexedVal, int upperBound, String column) throws ClassNotFoundException, IOException {
		IndexedValue iv = null;
		
		iv = checkIndex(indexedVal, upperBound, column);
		
		List<Integer> rowIds = iv.getRowIds();
		
		if (rowIds.size() > 1) {
			rowIds.remove(rowId);
			putDST(iv, upperBound, column);
		} else if (rowIds.size() == 1) {
			removeDST(iv, upperBound, column);
		} else {
			logger.error("Indexed value not found in the index!");
		}
	}

	protected IndexedValue checkIndex(int indexedVal, int upperBound, String column) throws ClassNotFoundException, IOException {
		
		Map<Integer, IndexedValue> results = getDSTblocking(indexedVal, indexedVal, upperBound, column);
		
		if (results.size() > 0) {
			return results.get(indexedVal);
		}
		
		return null;
	}
	
	
	
	protected void putDST(IndexedValue indexedVal, int upperBound, String column) throws IOException {
	  
		  DSTBlock block = new DSTBlock(1, upperBound, column);
		  
		  for (int i = 0; i <= Utils.getDSTHeight(upperBound); i++) {
//			  logger.debug("PUT INTERVAL: "+block.toString());
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
		              }
		          });
		      block = block.split(indexedVal.getIndexedVal());
		  }
	 
	}
	
	private void removeDST(IndexedValue iv, int upperBound, String column) {
		DSTBlock block = new DSTBlock(1, upperBound, column);
		  
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
	              }
	          });
	      block = block.split(iv.getIndexedVal());
		}
		
	}
	
	public Map<Integer, IndexedValue> getDSTblocking(int from, int to, int upperBound, String column) throws ClassNotFoundException, IOException {
      List<DSTBlock> rowsBlocks = Utils.splitRange(from, to, upperBound, column);
      Map<Integer, IndexedValue> results = new HashMap<>();
      AtomicInteger counter = new AtomicInteger(0);
      
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

		for (final DSTBlock block : blocks) {
			// logger.debug("GET INTERVAL: "+interval.toString());

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
							getDST(block.split(), upperBound, already, results,
									counter, ih);
						}

						if (counter.decrementAndGet() == 0) {
							synchronized (ih) {
								ih.notify();
							}
						}

					} else {
						// add exception?
						logger.debug("GET DST: Get failed!");
						if (counter.decrementAndGet() == 0) {
							synchronized (ih) {
								ih.notify();
							}
						}
					}
				}
			});
		}

	}

}
