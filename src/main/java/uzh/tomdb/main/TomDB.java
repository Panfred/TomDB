
package uzh.tomdb.main;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Random;

import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uzh.tomdb.api.Connection;
import uzh.tomdb.p2p.DBPeer;

/**
 *
 * Main TomDB static class to create the peers and get the connection to the database API.
 *
 * @author Francesco Luminati
 */
public class TomDB {
    private static final Logger logger = LoggerFactory.getLogger(TomDB.class);
    /**
     * Database peer.
     */
    private static DBPeer DBpeer;
    /**
     * Local DHT peers.
     */
    private static Peer[] peers = new Peer[0];
    /**
     * Default port.
     */
    private static int port = 4000;
    private static Random rnd = new Random();
    
    /**
     * Start TomDB as the bootstrapping peer. (non-API)
     */
    public static void startDHT() {
    	if (peers.length == 0) {
    		createLocalPeers(1);
    		logger.info("Succesfully created the bootstrapping peer with the address {} and port {}!", peers[0].getPeerAddress().getInetAddress().getHostAddress(), peers[0].getPeerAddress().portUDP());
    	} else {
    		logger.info("The bootstrapping peer has the address {} and port {}!", peers[0].getPeerAddress().getInetAddress().getHostAddress(), peers[0].getPeerAddress().portUDP());
    	}
    }
    
    /**
     * Start a peer and bootstrap to the given address. (non-API)
     * If randomPort is true, a random port is chosen to avoid conflicts if many peers are executed on the same machine.
     * 
     * @param bootstrapAddress
     * @param randomPort
     */
    public static void startDHT(String bootstrapAddress, boolean randomPort) {
    	if (randomPort) {
    		port = 4000 + (rnd.nextInt() % 1000);
    	}
    	if (peers.length == 0) {
    		createLocalPeers(1);
    		bootstrap(bootstrapAddress);
    		logger.info("Succesfully created and bootstrapped one peer!");
    	} else {
    		bootstrap(bootstrapAddress);
    		logger.info("Succesfully bootstrapped local peers!");
    	}
    }
    
    /**
     * Start TomDB as the bootstrapping peer and crates a DB peer, returning the connection object. (API)
     */
	public static Connection getConnection() {
        if (DBpeer == null) {
        	if (peers.length == 0) {
        		createLocalPeers(1);
        		DBpeer = new DBPeer(peers);
        		logger.info("Succesfully created the bootstrapping peer and the DB peer with the address {} and port {}!", peers[0].getPeerAddress().getInetAddress().getHostAddress(), peers[0].getPeerAddress().portUDP());
        	} else {
        		DBpeer = new DBPeer(peers);
        		logger.info("Succesfully created the DB peer and the bootstrapping peer has the address {} and port {}!", peers[0].getPeerAddress().getInetAddress().getHostAddress(), peers[0].getPeerAddress().portUDP());
        	}
            return DBpeer.getConnection();
        } else {
        	logger.info("DB peer already created!");
            return DBpeer.getConnection();
        }  
    }
    
	/**
	 * Start a peer and bootstrap to the given address, start a DB peer returning the connection object. (API)
	 * If randomPort is true, a random port is chosen to avoid conflicts if many peers are executed on the same machine.
	 * 
	 * @param bootstrapAddress
	 * @param randomPort
	 */
    public static Connection getConnection(String bootstrapAddress, boolean randomPort) {    	
    	if (DBpeer == null) {
    		if (randomPort) {
        		port = 4000 + (rnd.nextInt() % 1000);
        	}
        	if (peers.length == 0) {
        		createLocalPeers(1);
        		bootstrap(bootstrapAddress);
        		DBpeer = new DBPeer(peers);
        		logger.info("Succesfully created and bootstrapped one peer and created the DB peer!");
        	} else {
        		bootstrap(bootstrapAddress);
        		DBpeer = new DBPeer(peers);
        		logger.info("Succesfully bootstrapped local peers and created the DB peer!");
        	}
            return DBpeer.getConnection();
        } else {
        	logger.info("DB peer already created!");
            return DBpeer.getConnection();
        }  
    }
    
    /**
     * Bootstrap to the given address on default port 4000.
     * 
     * @param inetAddress
     */
    private static void bootstrap(String inetAddress) {
    	FutureBootstrap fb = null;
		try {
			fb = peers[0].bootstrap().setInetAddress(Inet4Address.getByName(inetAddress)).setPorts(4000).start();
		} catch (UnknownHostException e) {
			logger.error("InetAddress conversion failed!", e);
		}
        fb.awaitUninterruptibly();
        if(fb.isFailed()) {
            logger.debug("Bootstrap failed!");
        } else {
            logger.debug("Bootstrap successfull!");
            if (fb.getBootstrapTo() != null) {
            	peers[0].discover().setPeerAddress(fb.getBootstrapTo().iterator().next()).start().awaitUninterruptibly();
            }
        }
		
	}
    
    /**
     * Shutdown the peers and exits.
     */
	public static void stopDHT() {
    	peers[0].shutdown();
    	logger.debug("Closing peers down...");
    	System.exit(0);
    }
    
	/**
	 * Create n local peers on the same port.
	 * @param num
	 */
    public static void createLocalPeers(int num) {
    	
        peers = new Peer[num];
        for ( int i = 0; i < num; i++ )
        {
            if ( i == 0 )
            {
                try {
                    peers[0] = new PeerMaker(new Number160(rnd)).setPorts(port).makeAndListen();
                } catch (IOException ex) {
                    logger.error("Failed to start a new Peer", ex);
                }
            }
            else
            {
                try {
                    peers[i] = new PeerMaker( new Number160(rnd)).setMasterPeer(peers[0]).makeAndListen();
                } catch (IOException ex) {
                    logger.error("Failed to start a new Peer", ex);
                }
            }
        }
        if (peers.length > 1) {
        	for (int i=0; i<num;i++) {
            	for(int j=0; j<num; j++) {
            		peers[i].getPeerBean().getPeerMap().peerFound(peers[j].getPeerAddress(), null);
            	}
            }
        }
        logger.debug("Successfully started {} local Peers!", num);
    }
    
}
