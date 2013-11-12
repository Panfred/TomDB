/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uzh.tomdb.main;

import java.io.IOException;
import java.util.Random;

import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uzh.tomdb.api.Connection;
import uzh.tomdb.p2p.DBPeer;

/**
 *
 * @author Panfred
 */
public class TomDB {
    private static final Logger logger = LoggerFactory.getLogger(TomDB.class);
    private static DBPeer peer;
    private static Peer[] peers;
    private static Random rnd = new Random();
    
    public TomDB() { 
    }
    
    public static Connection getConnection() {
        if (peer == null) {
            try {
                if (peers != null) {
                    peer = new DBPeer(rnd.nextInt(), peers);
                } else {
                    peer = new DBPeer(rnd.nextInt());
                }   
            } catch (IOException ex) {
                logger.error("Failed to start a new Peer", ex);
            }
            return peer.getConnection();
        } else {
            return peer.getConnection();
        }  
    }
    
    public static Peer[] createLocalPeers(int num) {
        peers = new Peer[num];
        for ( int i = 0; i < num; i++ )
        {
            if ( i == 0 )
            {
                try {
                    peers[0] = new PeerMaker(new Number160(rnd)).setPorts(4000).makeAndListen();
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
        for (int i=0; i<num;i++) {
        	for(int j=0; j<num; j++) {
        		peers[i].getPeerBean().getPeerMap().peerFound(peers[j].getPeerAddress(), null);
        	}
        }
        return peers;
    }
    
}
