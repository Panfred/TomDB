package uzh.tomdb.db.operations.engines;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDHT;
import uzh.tomdb.db.operations.Select;
import uzh.tomdb.db.operations.helpers.Block;
import uzh.tomdb.db.operations.helpers.ConditionsHandler;
import uzh.tomdb.db.operations.helpers.Utils;

public class TableScan {
	private final Logger logger = LoggerFactory.getLogger(TableScan.class);  
	private Select select;
	private ConditionsHandler condHandler;
	
	public TableScan(Select select, ConditionsHandler ch) {
		this.select = select;
		this.condHandler = ch;
	}
	
	public void start() {
		List<Block> blocks = Utils.getBlocks(0, select.getTr().getRowsNum(), select.getTr().getRowsNum(), select.getTr().getBlockSize());
		
		for (Block block : blocks) {
			
			FutureDHT future = select.getPeer().get(block.getHash()).setAll().start();
			condHandler.addToFutureManager(future.toString());
			future.addListener(new BaseFutureAdapter<FutureDHT>() {
				@Override
				public void operationComplete(FutureDHT future) throws Exception {
					if (future.isSuccess()) {
						logger.debug("GET TableScan: Get succeed!");
						condHandler.filterRows(future.getDataMap(), future.toString());
					} else {
						// add exception?
						logger.debug("GET TableScan: Get failed!");
						condHandler.removeFromFutureManager(future.toString());
					}
				}
				
			});
			
		}
	}

}
