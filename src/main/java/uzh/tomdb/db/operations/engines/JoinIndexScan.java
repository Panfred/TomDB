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

public class JoinIndexScan {
	private final Logger logger = LoggerFactory.getLogger(JoinIndexScan.class); 
	private Select select;
	private JoinsHandler handler;
	private JoinCondition jCond;
	
	public JoinIndexScan(Select select, JoinsHandler handler, JoinCondition jCond) {
		this.select = select;
		this.handler = handler;
		this.jCond = jCond;
	}
	
	public void start() throws MalformedSQLQuery, ClassNotFoundException, IOException {
		
		List<DSTBlock> rowsBlocks = Utils.splitRange(select.getTi().getMin(jCond.getColumn(select.getTabName())), select.getTi().getMax(jCond.getColumn(select.getTabName())), select.getTi().getDSTRange(), jCond.getColumn(select.getTabName()));
		
		getDST(rowsBlocks, new HashSet<String>());
		
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
