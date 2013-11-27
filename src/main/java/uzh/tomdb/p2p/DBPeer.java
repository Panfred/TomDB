
package uzh.tomdb.p2p;

import java.io.IOException;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.SortedMap;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.StorageGeneric;
import net.tomp2p.storage.StorageMemory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uzh.tomdb.api.Connection;
import uzh.tomdb.db.TableIndexes;
import uzh.tomdb.db.TableRows;


/**
 *
 * @author Francesco Luminati
 */
public class DBPeer {
    private static final Logger logger = LoggerFactory.getLogger(DBPeer.class);
    private static Peer peer;
    private static Peer[] localPeers;
    private static Map<Number160, Data> tabColumns = new HashMap<>();
    private static Map<Number160, Data> tabRows = new HashMap<>();
    private static Map<Number160, Data> tabIndexes = new HashMap<>();
    private static boolean setupStorage = false;
    private static long timeRows;
    private static long timeIndexes;
    
    public DBPeer(Random rnd) throws IOException {
    	peer = new PeerMaker(new Number160(rnd)).setPorts(4000 + (rnd.nextInt() % 1000)).makeAndListen();
        FutureBootstrap fb = peer.bootstrap().setBroadcast().setPorts(4000).start();
        fb.awaitUninterruptibly();
        if(fb.isFailed()) {
            logger.debug("Bootstrap failed, becoming bootstrap peer...");
            peer = new PeerMaker(new Number160(rnd)).setPorts(4000).makeAndListen();
        } else {
            logger.debug("Bootstrap successfull!");
            if (fb.getBootstrapTo() != null) {
                peer.discover().setPeerAddress(fb.getBootstrapTo().iterator().next()).start().awaitUninterruptibly();
            }
        }
    }
    
    public DBPeer(Random rnd, Peer[] peers) throws IOException {
        localPeers = peers;
    	peer = new PeerMaker(new Number160(rnd)).setPorts(4000 + (rnd.nextInt() % 1000)).makeAndListen();
        FutureBootstrap fb = peer.bootstrap().setPeerAddress(localPeers[0].getPeerAddress()).start();
        fb.awaitUninterruptibly();
        if(fb.isFailed()) {
            logger.debug("Bootstrap failed!");
        } else {
            logger.debug("Bootstrap successfull!");
            if (fb.getBootstrapTo() != null) {
                peer.discover().setPeerAddress(fb.getBootstrapTo().iterator().next()).start().awaitUninterruptibly();
            }
        }
    }

    public Connection getConnection() {
    	fetchTableRows();
        fetchTableColumns();
        fetchTableIndexes();
        return new Connection();
    }
   
	public static Peer getPeer() {
        return peer;
    }
    
    /**
     * 
     * @return
     */
    public static Map<Number160, Data> getTabColumns() {
        return tabColumns;
    }
    
    /**
     * FETCH after 1 min
     * @return
     */
    public static Map<Number160, Data> getTabRows() {
    	if (timeRows < System.currentTimeMillis()) {
    		fetchTableRows();
    	}
        return tabRows;
    }

    /**
     * FETCH after 1 min
     * @return
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
     * Blocking
     */
    public static void fetchTableColumns() {
		FutureDHT future = peer.get(Number160.createHash("TableColumnsMetaData")).setAll().start();
		future.awaitUninterruptibly();
		if (future.isSuccess()) {
			tabColumns = future.getDataMap();
			logger.debug("Success fetching Table COLUMNS");
		} else {
			// add exception?
			logger.debug("Failure fetching Table COLUMNS!");
		}
    }
    
    /**
     * Blocking
     */
    public static void fetchTableRows() {
        FutureDHT future = peer.get(Number160.createHash("TableRowsMetaData")).setAll().start();
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
    }
    
    /**
     * Blocking
     */
    public static void fetchTableIndexes() {
        FutureDHT future = peer.get(Number160.createHash("TableIndexesMetaData")).setAll().start();
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
    }
    
    public static void updateTableColumns() {
        
        FutureDHT future = peer.put(Number160.createHash("TableColumnsMetaData")).setDataMap(tabColumns).start();
        future.addListener(new BaseFutureAdapter<FutureDHT>() {
            @Override
            public void operationComplete(FutureDHT future) throws Exception {
                if (future.isSuccess()) {
                    logger.debug("Success updateing COLUMNS metadata!");
                } else {
                    //add exception?
                    logger.debug("Failed updateing COLUMNS metadata!");
                }
            }
        });
        
        //TODO BROADCAST!!! TABLE COLUMNS METADATA are updated only with CREATE TABLE, a client must EXPLICIT call FETCH METADATA to see a new table     
    }
    
    public static void updateTableRows() {
    	updateTableRows(tabRows);
    }
    
    public static void updateTableRows(Map<Number160, Data> tabRows) {
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
			}
		});

	}
    
    public static void updateTableIndexes(){
    	updateTableIndexes(tabIndexes);
    }
    
    public static void updateTableIndexes(Map<Number160, Data> tabIndexes) {

        FutureDHT future = peer.put(Number160.createHash("TableIndexesMetaData")).setDataMap(tabIndexes).start();
        future.addListener(new BaseFutureAdapter<FutureDHT>() {
            @Override
            public void operationComplete(FutureDHT future) throws Exception {
                if (future.isSuccess()) {
                    logger.debug("Success updateing INDEXES metadata!");
                } else {
                    //add exception?
                    logger.debug("Failed updateing INDEXES metadata!");
				}
			}
		});

	}
    
    /**
     * Set Storage capacity of a peer.
     * Takes the DSTRange of the first table, because it is not possible to define different storage capacity for different tables...
     * 
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
     * Adds a custom storage class that has a limited storage size according to the maxBlockSize.
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
            
            peer.getPeerBean().setStorage(sm);
            
            if (localPeers != null) {
                for (Peer peers : localPeers) {
                    peers.getPeerBean().setStorage(sm);
                }
            }
    }
}
