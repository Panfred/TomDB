package uzh.tomdb.db.operations.engines;

import java.io.IOException;
import java.util.Map;

import uzh.tomdb.parser.MalformedSQLQuery;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

/**
 * 
 * Interface for the handlers, mainly used to handle asynchronous DHT operations.
 * 
 * @author Francesco Luminati
 */
public interface Handler {
	public void addToFutureManager(String future);
	public void removeFromFutureManager(String future);
	public void filterRows(Map<Number160, Data> dataMap, String future) throws ClassNotFoundException, IOException, NumberFormatException, MalformedSQLQuery;
}
