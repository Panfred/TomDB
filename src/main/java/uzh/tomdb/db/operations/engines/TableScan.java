
package uzh.tomdb.db.operations.engines;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDHT;
import uzh.tomdb.db.operations.Select;
import uzh.tomdb.db.operations.helpers.Block;
import uzh.tomdb.db.operations.helpers.Utils;

/**
 * 
 * TableScan operation to retrieve all the rows from the table.
 * 
 * @author Francesco Luminati
 */
public class TableScan {
	private final Logger logger = LoggerFactory.getLogger(TableScan.class);
	/**
	 * Select object to get MetaData information.
	 */
	private Select select;
	/**
	 * The handler can be a ConditionsHandler or a JoinsHandler, used to forward the resulting rows.
	 */
	private Handler handler;
	
	public TableScan(Select select, Handler handler) {
		this.select = select;
		this.handler = handler;
	}
	
	/**
	 * DHT get operation to get all the blocks of a table and send them to the handler.
	 */
	public void start() {
		List<Block> blocks = Utils.getBlocks(0, select.getTr().getRowsNum(), select.getTr().getRowsNum(), select.getTr().getBlockSize(), select.getTabName());
				
			for (Block block : blocks) {
				
				FutureDHT future = select.getPeer().get(block.getHash()).setAll().start();
				handler.addToFutureManager(future.toString());
				
				future.addListener(new BaseFutureAdapter<FutureDHT>() {
					@Override
					public void operationComplete(FutureDHT future) throws Exception {
						if (future.isSuccess()) {
							logger.debug("GET TableScan: Get succeed!");
							handler.filterRows(future.getDataMap(), future.toString());		
						} else {
							// add exception?
							logger.debug("GET TableScan: Get failed!");
							handler.removeFromFutureManager(future.toString());
						}
					}
					
				});
				
			}
			
	}

}

	
