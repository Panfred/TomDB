package uzh.tomdb.db.operations.engines;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uzh.tomdb.db.TableIndexes;
import uzh.tomdb.db.indexes.DSTBlock;
import uzh.tomdb.db.indexes.IndexedValue;
import uzh.tomdb.db.operations.Select;
import uzh.tomdb.db.operations.helpers.AndCondition;
import uzh.tomdb.db.operations.helpers.Block;
import uzh.tomdb.db.operations.helpers.Conditions;
import uzh.tomdb.db.operations.helpers.ConditionsHandler;
import uzh.tomdb.db.operations.helpers.Utils;
import uzh.tomdb.db.operations.helpers.WhereCondition;
import uzh.tomdb.parser.MalformedSQLQuery;
import uzh.tomdb.parser.Tokens;

/**
 * 
 * @author Francesco Luminati
 *
 */
public class IndexScan {
	private final Logger logger = LoggerFactory.getLogger(IndexScan.class);  
	private Select select;
	private ConditionsHandler condHandler;
	private Set<String> elaboratedBlocks = new HashSet<>();
	
	public IndexScan(Select select, ConditionsHandler ch) {
		this.select = select;
		this.condHandler = ch;
	}
	
	public void start() throws MalformedSQLQuery, ClassNotFoundException, IOException {
		boolean indexExistence = false;

		for (Conditions condition : condHandler.getAndCond()) {

			if (checkIndexExistence(condition)) {
				indexExistence = true;
				indexScan(condition);
				break;
			}

		}

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
		
		List<DSTBlock> rowsBlocks = Utils.splitRange(from, to, select.getTi().getDSTRange(), cond.getColumn());
		
		getDST(rowsBlocks, new HashSet<String>());
		
	}

	private void filterIndexScan(Map<Number160, Data> map) throws ClassNotFoundException, IOException {
		for(Map.Entry<Number160, Data> entry: map.entrySet()) {
			IndexedValue iv = (IndexedValue) entry.getValue().getObject();
			tableScan(iv.getRowIds());
		}
	}
	
	private void tableScan(List<Integer> rowIds) {
		List<Block> blocks = new ArrayList<>();
		
		for (Integer id: rowIds) {
			List<Block> ls = Utils.getBlocks(id, id, select.getTr().getRowsNum(), select.getTr().getBlockSize());
			Block block = ls.get(0);
			if (!elaboratedBlocks.contains(block.toString())) {
				elaboratedBlocks.add(block.toString());
				blocks.add(block);
			}
		}
		
		for (Block block2: blocks) {
			logger.debug(block2.toString());
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
				}	
			});
		}
		
	}
	
	private boolean checkIndexExistence(Conditions condition) {
		WhereCondition cond = (WhereCondition) condition;
		TableIndexes ti = select.getTi();
		
		if (ti.getIndexes().contains(cond.getColumn()) || ti.getUniqueIndexes().contains(cond.getColumn())) {
			return true;
		}
		
		return false;
	}
	
	 private void getDST(List<DSTBlock> blocks, final Set<String> already) throws ClassNotFoundException, IOException {
		 	
		 	for (final DSTBlock block: blocks) {
	    	  //logger.debug("GET INTERVAL: "+interval.toString());
	          
	    	  // we don't query the same thing again
	          if (already.contains(block.toString())) {
	              continue;
	          }

	          Number160 key = block.getHash();
	          already.add(block.toString());

	          // get the interval
	          FutureDHT future = select.getPeer().get(key).setAll().start();
	          condHandler.addToFutureManager(future.toString());
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
	                          getDST(block.split(), already);
	                      }
	                      
	                  } else {
	                      //add exception?
	                	  condHandler.removeFromFutureManager(future.toString());
	                      logger.debug("GET DST: Get failed!");
	                  }
	              }
	          });          
	      }
	  }
	
}
