
package uzh.tomdb.p2p;

import java.io.IOException;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.StorageGeneric;
import net.tomp2p.storage.StorageMemory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uzh.tomdb.api.Connection;
import uzh.tomdb.api.Statement;
import uzh.tomdb.db.TableIndexes;
import uzh.tomdb.db.TableRows;


/**
 *
 * Database peer, responsible for the table MetaData.
 *
 * @author Francesco Luminati
 */
public class DBPeer {
    private static final Logger logger = LoggerFactory.getLogger(DBPeer.class);
    /**
     * The peer used to do all the DHT operations.
     */
    private static Peer peer;
    /**
     * Array of the local peers.
     */
    private static Peer[] localPeers;
    /**
     * MetaData about table Columns.
     */
    private static Map<Number160, Data> tabColumns = new HashMap<>();
    /**
     * MetaData about table Rows.
     */
    private static Map<Number160, Data> tabRows = new HashMap<>();
    /**
     * MetaData about table Indexes.
     */
    private static Map<Number160, Data> tabIndexes = new HashMap<>();
    private static boolean setupStorage = false;
    private static long timeRows;
    private static long timeIndexes;
    
    public DBPeer(Peer[] peers) {
    	peer = peers[0];
    	localPeers = peers;
    }
    
    /**
     * Setup the MetaData from the DHT and returns a connection object.
     */
    public Connection getConnection() {
    	fetchTableRows();
        fetchTableColumns();
        fetchTableIndexes();
        return new Connection();
    }
   
	public static Peer getPeer() {
        return peer;
    }
    
    public static Map<Number160, Data> getTabColumns() {
        return tabColumns;
    }
    
    /**
     * FETCH if older than 1 min.
     */
    public static Map<Number160, Data> getTabRows() {
    	if (timeRows < System.currentTimeMillis()) {
    		fetchTableRows();
    	}
        return tabRows;
    }

    /**
     * FETCH if older than 1 min.
     */
    public static Map<Number160, Data> getTabIndexes() {
    	if (timeIndexes < System.currentTimeMillis()) {
    		fetchTableIndexes();
    	}
        return tabIndexes;
    }
    
    public static void addTabColumn(Number160 key, Data data) {
        DBPeer.tabColumns.put(key, data);
    }

    public static void addTabRow(Number160 key, Data data) {
        DBPeer.tabRows.put(key, data);
    }
    
    public static void addTabIndex(Number160 key, Data data) {
        DBPeer.tabIndexes.put(key, data);
    }
    
    /**
     * Blocking operation to get the MetaData from the DHT.
     */
    public static void fetchTableColumns() {
		FutureDHT future = peer.get(Number160.createHash("TableColumnsMetaData")).setAll().start();
		logger.trace("METADATA-FETCH-COLUMNS", "BEGIN", Statement.experiment, future.hashCode());
		future.awaitUninterruptibly();
		if (future.isSuccess()) {
			tabColumns = future.getDataMap();
			logger.debug("Success fetching Table COLUMNS");
		} else {
			// add exception?
			logger.debug("Failure fetching Table COLUMNS!");
		}
		logger.trace("METADATA-FETCH-COLUMNS", "END", Statement.experiment, future.hashCode());
    }
    
    /**
     * Blocking operation to get the MetaData from the DHT.
     */
    public static void fetchTableRows() {
        FutureDHT future = peer.get(Number160.createHash("TableRowsMetaData")).setAll().start();
        logger.trace("METADATA-FETCH-ROWS", "BEGIN", Statement.experiment, future.hashCode());
        future.awaitUninterruptibly();
        if(future.isSuccess()) {
            tabRows = future.getDataMap();
            timeRows = System.currentTimeMillis() + 60000; // 1 min
            logger.debug("Success fetching Table ROWS");
            logger.debug("FUTURE ROUTE fetch: " + future.getFutureRouting().getRoutingPath());
            if(!tabRows.isEmpty()) {
            	try {
            		Iterator<Entry<Number160, Data>> it = tabRows.entrySet().iterator();
            		while(it.hasNext()) {
            			logger.debug("Rows Metadata: "+((TableRows) it.next().getValue().getObject()).toString());
            		}
        		} catch (ClassNotFoundException | IOException e) {
        			logger.error("Data error", e);
        		}
            }
        } else {
            //add exception?
            logger.debug("Failure fetching Table ROWS!");
        }
        logger.trace("METADATA-FETCH-ROWS", "END", Statement.experiment, future.hashCode());
    }
    
    /**
     * Blocking operation to get the MetaData from the DHT.
     */
    public static void fetchTableIndexes() {
        FutureDHT future = peer.get(Number160.createHash("TableIndexesMetaData")).setAll().start();
        logger.trace("METADATA-FETCH-INDEXES", "BEGIN", Statement.experiment, future.hashCode());
        future.awaitUninterruptibly();
        if(future.isSuccess()) {
            tabIndexes = future.getDataMap();
            timeIndexes = System.currentTimeMillis() + 60000; // 1 min
            if (!setupStorage) {
            	doSetupStorage();
            }
            logger.debug("Success fetching Table INDEXES");
        } else {
            //add exception?
            logger.debug("Failure fetching Table INDEXES!");
        }
        logger.trace("METADATA-FETCH-INDEXES", "END", Statement.experiment, future.hashCode());
    }
    
    /**
     * Non-blocking operation to put the MetaData in the DHT.
     */
    public static void updateTableColumns() {
        FutureDHT future = peer.put(Number160.createHash("TableColumnsMetaData")).setDataMap(tabColumns).start();
        logger.trace("METADATA-UPDATE-COLUMNS", "BEGIN", Statement.experiment, future.hashCode());
        future.addListener(new BaseFutureAdapter<FutureDHT>() {
            @Override
            public void operationComplete(FutureDHT future) throws Exception {
                if (future.isSuccess()) {
                    logger.debug("Success updateing COLUMNS metadata!");
                } else {
                    //add exception?
                    logger.debug("Failed updateing COLUMNS metadata!");
                }
                logger.trace("METADATA-UPDATE-COLUMNS", "END", Statement.experiment, future.hashCode());
            }
        });
        
        //TODO BROADCAST!!! TABLE COLUMNS METADATA are updated only with CREATE TABLE, a client must EXPLICIT call FETCH METADATA to see a new table     
    }
    
    /**
     * Non-blocking operation to put the MetaData in the DHT.
     */
    public static void updateTableRows() {
    	if(!tabRows.isEmpty()) {
        	try {
        		Iterator<Entry<Number160, Data>> it = tabRows.entrySet().iterator();
        		while(it.hasNext()) {
        			logger.debug("Rows Metadata before update: "+((TableRows) it.next().getValue().getObject()).toString());
        		}
    		} catch (ClassNotFoundException | IOException e) {
    			logger.error("Data error", e);
    		}
        }
        FutureDHT future = peer.put(Number160.createHash("TableRowsMetaData")).setDataMap(tabRows).start();
        logger.trace("METADATA-UPDATE-ROWS", "BEGIN", Statement.experiment, future.hashCode());
        future.addListener(new BaseFutureAdapter<FutureDHT>() {
            @Override
            public void operationComplete(FutureDHT future) throws Exception {
                if (future.isSuccess()) {
                    logger.debug("Success updateing ROWS metadata!");
                    logger.debug("FUTURE ROUTE Update: " + future.getFutureRouting().getRoutingPath());
                } else {
                    //add exception?
                    logger.debug("Failed updateing ROWS metadata!");
				}
                logger.trace("METADATA-UPDATE-ROWS", "END", Statement.experiment, future.hashCode());
			}
		});

	}
    
    /**
     * Non-blocking operation to put the MetaData in the DHT.
     */
    public static void updateTableIndexes() {

        FutureDHT future = peer.put(Number160.createHash("TableIndexesMetaData")).setDataMap(tabIndexes).start();
        logger.trace("METADATA-UPDATE-INDEXES", "BEGIN", Statement.experiment, future.hashCode());
        future.addListener(new BaseFutureAdapter<FutureDHT>() {
            @Override
            public void operationComplete(FutureDHT future) throws Exception {
                if (future.isSuccess()) {
                    logger.debug("Success updateing INDEXES metadata!");
                } else {
                    //add exception?
                    logger.debug("Failed updateing INDEXES metadata!");
				}
                logger.trace("METADATA-UPDATE-INDEXES", "END", Statement.experiment, future.hashCode());
			}
		});

	}
    
    /**
     * Set Storage capacity of a peer.
     * Takes the DSTRange of the first table, because it is not possible to define different storage capacity for different tables.
     */
    private static void doSetupStorage() {
		try {
			TableIndexes index = (TableIndexes) tabIndexes.entrySet().iterator().next().getValue().getObject();
			setupStorage(index.getDSTRange());
		} catch (ClassNotFoundException | IOException e) {
			logger.error("Data error", e);
		}
		setupStorage = true;
	}
    
    /**
     * Adds a custom storage class that has a limited storage size according to the blockSize.
     * 
     * @param blockSize
     *            The max. number of elements per node.
     */
    private static void setupStorage(final int blockSize) {    
            StorageMemory sm = new StorageMemory() {
                @Override
                public StorageGeneric.PutStatus put(final Number160 locationKey, final Number160 domainKey,
                        final Number160 contentKey, final Data newData, final PublicKey publicKey,
                        final boolean putIfAbsent, final boolean domainProtection) {
                    Map<Number480, Data> map = subMap(locationKey, domainKey, Number160.ZERO, Number160.MAX_VALUE);
                    if (map.size() < blockSize) {
                        return super.put(locationKey, domainKey, contentKey, newData, publicKey, putIfAbsent,
                                domainProtection);
                    } else {
                        return StorageGeneric.PutStatus.FAILED;
                    }
                } 
                
                @Override
                public SortedMap<Number480, Data> get(final Number160 locationKey, final Number160 domainKey,
                        final Number160 fromContentKey, final Number160 toContentKey) {
                    return super.get(locationKey, domainKey, fromContentKey, toContentKey);   
                }
            };
            
            for (Peer peers: localPeers) {
                peers.getPeerBean().setStorage(sm);
            }
            
    }
}
