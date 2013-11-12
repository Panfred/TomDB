package uzh.tomdb.db.operations;

import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import uzh.tomdb.db.TableColumns;
import uzh.tomdb.db.TableIndexes;
import uzh.tomdb.db.TableRows;
import uzh.tomdb.p2p.DBPeer;

public class Operation {
	protected Peer peer;
	protected String tabName;
	protected Number160 tabKey;
	protected TableRows tr;
	protected TableColumns tc;
	protected TableIndexes ti;

	public Operation() {
		this.peer = DBPeer.getPeer();
	}

	public Peer getPeer() {
		return peer;
	}

	public String getTabName() {
		return tabName;
	}

	public Number160 getTabKey() {
		return tabKey;
	}

	public TableRows getTr() {
		return tr;
	}

	public TableColumns getTc() {
		return tc;
	}

	public TableIndexes getTi() {
		return ti;
	}

	@Override
	public String toString() {
		return "Operation [peer=" + peer + ", tabName=" + tabName + ", tabKey="
				+ tabKey + ", tr=" + tr + ", tc=" + tc + ", ti=" + ti + "]";
	}
	
	
}