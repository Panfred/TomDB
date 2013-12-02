
package uzh.tomdb.db.operations.engines;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;
import uzh.tomdb.db.indexes.DSTBlock;
import uzh.tomdb.db.operations.Select;
import uzh.tomdb.db.operations.helpers.JoinCondition;
import uzh.tomdb.db.operations.helpers.Utils;
import uzh.tomdb.parser.MalformedSQLQuery;

/**
 * 
 * IndexScan for joins.
 * 
 * @author Francesco Luminati
 */
public class JoinIndexScan {
	private final Logger logger = LoggerFactory.getLogger(JoinIndexScan.class); 
	/**
	 * Select object to get MetaData information.
	 */
	private Select select;
	/**
	 * The handler to return the results back.
	 */
	private JoinsHandler handler;
	/**
	 * The join condition to know for which column an indexscan is necessary.
	 */
	private JoinCondition jCond;
	
	public JoinIndexScan(Select select, JoinsHandler handler, JoinCondition jCond) {
		this.select = select;
		this.handler = handler;
		this.jCond = jCond;
	}
	
	/**
	 * Start the indexscan of the entire index on the DST, using min/max value of index MetaData as range.
	 */
	public void start() throws MalformedSQLQuery, ClassNotFoundException, IOException {
		
		List<DSTBlock> rowsBlocks = Utils.splitRange(select.getTi().getMin(jCond.getColumn(select.getTabName())), select.getTi().getMax(jCond.getColumn(select.getTabName())), select.getTi().getDSTRange(), jCond.getColumn(select.getTabName()));
		
		getDST(rowsBlocks, new HashSet<String>());
		
	}
	
	/**
	 * Recursive operation to get the DST blocks. Recursion happens only when a DST block is full.
	 * 
	 * @param blocks
	 * @param already
	 */
	private void getDST(List<DSTBlock> blocks, final Set<String> already) throws ClassNotFoundException, IOException {
	 	
	 	for (final DSTBlock block: blocks) {
          
    	  // we don't query the same thing again
          if (already.contains(block.toString())) {
              continue;
          }

          Number160 key = block.getHash();
          already.add(block.toString());

          // get the interval
          FutureDHT future = select.getPeer().get(key).setAll().start();
          handler.addToFutureManager(future.toString());
          future.addListener(new BaseFutureAdapter<FutureDHT>() {
              @Override
              public void operationComplete(FutureDHT future) throws Exception {
                  if (future.isSuccess()) {
                      logger.debug("GET DST: Get succeed!");
                      
                      Map<Number160, Data> map = future.getDataMap();
                      
                      handler.filterIndex(map, select.getTabName(), future.toString());
                      
                      //IF block is full, we need to get children
                      if (map.size() == select.getTi().getDSTRange()) {
                          logger.debug(block + " FULL!");
                          getDST(block.split(), already);
                      }
                      
                  } else {
                      //add exception?
                	  handler.removeFromFutureManager(future.toString());
                      logger.debug("GET DST: Get failed!");
                  }
              }
          });          
      }
  }
}
