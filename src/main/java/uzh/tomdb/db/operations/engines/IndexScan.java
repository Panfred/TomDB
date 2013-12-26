
package uzh.tomdb.db.operations.engines;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uzh.tomdb.api.Statement;
import uzh.tomdb.db.TableIndexes;
import uzh.tomdb.db.indexes.DSTBlock;
import uzh.tomdb.db.indexes.IndexedValue;
import uzh.tomdb.db.operations.Select;
import uzh.tomdb.db.operations.helpers.AndCondition;
import uzh.tomdb.db.operations.helpers.Block;
import uzh.tomdb.db.operations.helpers.Conditions;
import uzh.tomdb.db.operations.helpers.Utils;
import uzh.tomdb.db.operations.helpers.WhereCondition;
import uzh.tomdb.parser.MalformedSQLQuery;
import uzh.tomdb.parser.Tokens;

/**
 * 
 * IndexScan operation to retrieve rows from the table based on the indexes and the where conditions.
 * 
 * @author Francesco Luminati
 */
public class IndexScan {
	private final Logger logger = LoggerFactory.getLogger(IndexScan.class); 
	/**
	 * Select object to get MetaData information.
	 */
	private Select select;
	/**
	 * ConditionHandler to get AND and OR conditions, i.e. on which column an indexscan must be performed.
	 */
	private ConditionsHandler condHandler;
	/**
	 * Save the elaborated indexed values in case that the DST block is full and it needs to retrieve more blocks (i.e. duplicates indexed values).
	 */
	private Set<Number160> elaboratedIndex = Collections.newSetFromMap(new ConcurrentHashMap<Number160, Boolean>());
	/**
	 * Save the elaborated table Blocks to avoid duplicate gets.
	 */
	private Set<String> elaboratedBlocks = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
	private int expHash;
	
	public IndexScan(Select select, ConditionsHandler handler) {
		this.select = select;
		this.condHandler = handler;
		this.expHash = select.hashCode();
	}
	
	/**
	 * Starts the necessary executions of indexscan if the index exists. 
	 */
	public void start() throws MalformedSQLQuery, ClassNotFoundException, IOException {
		
		logger.trace("INDEXSCAN-WHOLE", "BEGIN", Statement.experiment, expHash);
		
		boolean indexExistence = false;
		
		/**
		 * For AND conditions, when a column with index is found, one indexscan on that column is sufficient.
		 */
		for (Conditions condition : condHandler.getAndCond()) {

			if (checkIndexExistence(condition)) {
				indexExistence = true;
				indexScan(condition);
				break;
			}

		}
		
		/**
		 * For OR conditions, every column involved in the conditions must have an index and multiple indexscans are started.
		 */
		for (Conditions condition : condHandler.getOrCond()) {
			if (condition.getType().equals(Tokens.WHERE)) {
				
				if (checkIndexExistence(condition)) {
					indexExistence = true;
					indexScan(condition); // parallel calling if more OR indexes to check
				} else {
					throw new MalformedSQLQuery("The OR condition does not correspond to an index!");
				}
				
			} else {
				for (Conditions cond : ((AndCondition) condition).getConditions()) {
					
					if (checkIndexExistence(cond)) {
						indexExistence = true;
						indexScan(cond);
						break;
					}
					
				}
			}
		}

		if (!indexExistence) {
			throw new MalformedSQLQuery("The IdexScan does not correspond to an index!");
		}
	}
	
	/**
	 * For the indexscan, we have first to identify a range to scan on the DST, which is defined in the condition.
	 * 
	 * @param condition
	 */
	private void indexScan(Conditions condition) throws ClassNotFoundException, IOException {
		WhereCondition cond = (WhereCondition) condition;
		int from = 0;
		int to = 0;
		int value = Integer.parseInt(cond.getValue());
		
		switch (cond.getOperator()) {
		case Tokens.EQUAL:
			from = value;
			to = value;
			break;
		case Tokens.GREATER:
		case Tokens.GREATEREQUAL:
			from = value;
			to = select.getTi().getMax(cond.getColumn());
			break;
		case Tokens.LESS:
		case Tokens.LESSEQUAL:
			from = select.getTi().getMin(cond.getColumn());
			to = value;
			break;
		}
		
		List<DSTBlock> dstBlocks = Utils.splitRange(from, to, select.getTi().getDSTRange(), select.getTabName(), cond.getColumn());

		logger.trace("INDEXSCAN-GET-DST", "BEGIN", Statement.experiment, expHash, condition.hashCode(), dstBlocks.size());	
		
		getDST(dstBlocks, new HashSet<String>(), new AtomicInteger(0), condition.hashCode());
		
	}
	
	/**
	 * The row IDs coming from the indexscan are sent to the tablescan to actually get the rows out of the DHT table.
	 * 
	 * @param map coming from the indexscan.
	 */
	private void filterIndexScan(Map<Number160, Data> map) throws ClassNotFoundException, IOException {
		for(Map.Entry<Number160, Data> entry: map.entrySet()) {
			if (!elaboratedIndex.contains(entry.getKey())) {
				elaboratedIndex.add(entry.getKey());
				IndexedValue iv = (IndexedValue) entry.getValue().getObject();
				tableScan(iv.getRowIds());
			}
		}
	}
	
	/**
	 * For every row IDs coming from the indexscan, the corresponding table block is identified. 
	 * When the block has not been retrieved yet, a DHT get operation is started. The operation gives the results forward to the ConditionsHandler for further elaborations.
	 * 
	 * @param rowIds
	 */
	private void tableScan(final List<Integer> rowIds) {
		List<Block> blocks = new ArrayList<>();
		
		for (Integer id: rowIds) {
			List<Block> ls = Utils.getBlocks(id, id, select.getTr().getRowsNum(), select.getTr().getBlockSize(), select.getTabName());
			Block block = ls.get(0);
			if (!elaboratedBlocks.contains(block.toString())) {
				elaboratedBlocks.add(block.toString());
				blocks.add(block);
			}
		}
	
		final AtomicInteger counter = new AtomicInteger(blocks.size());
		if (counter.get() > 0) {
			logger.trace("INDEXSCAN-GET-TABLE", "BEGIN", Statement.experiment, expHash, rowIds.hashCode(), counter.get());
		}

		for (Block block : blocks) {
			
			FutureDHT future = select.getPeer().get(block.getHash()).setAll().start();
			condHandler.addToFutureManager(future.toString());
			future.addListener(new BaseFutureAdapter<FutureDHT>() {
				@Override
				public void operationComplete(FutureDHT future) throws Exception {
					if (future.isSuccess()) {
						logger.debug("GET from Table: Get succeed!");
						condHandler.filterRows(future.getDataMap(), future.toString());
					} else {
						// add exception?
						logger.debug("GET from Table: Get failed!");
						condHandler.removeFromFutureManager(future.toString());
					}
					if (counter.decrementAndGet() == 0) {
						logger.trace("INDEXSCAN-GET-TABLE", "END", Statement.experiment, expHash, rowIds.hashCode());
					}
					if (condHandler.getFutureManager().isEmpty()) {
						logger.trace("INDEXSCAN-WHOLE", "END", Statement.experiment, expHash);
					}
				}	
			});
		}
		
	}
	
	private boolean checkIndexExistence(Conditions condition) {
		WhereCondition cond = (WhereCondition) condition;
		TableIndexes ti = select.getTi();
		
		if (ti.getIndexes().contains(cond.getColumn()) || ti.getUnivocalIndexes().contains(cond.getColumn())) {
			return true;
		}
		
		return false;
	}
	
	/**
	 * Recursive operation to get the DST blocks. Recursion happens only when a DST block is full.
	 * 
	 * @param blocks
	 * @param already
	 */
	private void getDST(List<DSTBlock> blocks, final Set<String> already, final AtomicInteger counter, final int condHash) throws ClassNotFoundException, IOException {
		 	
		 	for (final DSTBlock block: blocks) {
	          
	    	  // we don't query the same thing again
	          if (already.contains(block.toString())) {
	              continue;
	          }

	          Number160 key = block.getHash();
	          already.add(block.toString());

	          // get the interval
	          FutureDHT future = select.getPeer().get(key).setAll().start();
	          condHandler.addToFutureManager(future.toString());
	          counter.incrementAndGet();
	          future.addListener(new BaseFutureAdapter<FutureDHT>() {
	              @Override
	              public void operationComplete(FutureDHT future) throws Exception {
	                  if (future.isSuccess()) {
	                      logger.debug("GET DST: Get succeed!");
	                      
	                      Map<Number160, Data> map = future.getDataMap();
	                      
	                      condHandler.removeFromFutureManager(future.toString());
	                      filterIndexScan(map);
	                      
	                      //IF block is full, we need to get children
	                      if (map.size() == select.getTi().getDSTRange()) {
	                          logger.debug(block + " FULL!");
	                          logger.trace("INDEXSCAN-GET-DST", "RECURSION", Statement.experiment, expHash, condHash, 2);	
	                          getDST(block.split(), already, counter, condHash);
	                      }
	                      
	                  } else {
	                      //add exception?
	                	  condHandler.removeFromFutureManager(future.toString());
	                      logger.debug("GET DST: Get failed!");
	                  }
	                  if (counter.decrementAndGet() == 0) {
	                	  logger.trace("INDEXSCAN-GET-DST", "END", Statement.experiment, expHash, condHash);
	                  }
	              }
	          });          
	      }
	  }
	
}
