
package uzh.tomdb.db.operations.helpers;

import net.tomp2p.peers.Number160;

/**
 * A helper class that stores the interval of Row forming a Block.
 *
 * @author Thomas Bocek / Francesco Luminati
 *
 */
public class Block {

    private final int from;
    private final int to;
    private final String table;


    /**
     * @param from Interval from
     * @param to Interval to
     */
    public Block(int from, int to, String table) {
        if (from > to) {
            throw new IllegalArgumentException("from cannot be greater than to");
        }
        this.from = from;
        this.to = to;
        this.table = table;
    }

    @Override
    public String toString() {
        return "Block:"+table+":[" + from + ".." + to + "]";
    }

    public Number160 getHash() {
        return Number160.createHash(toString());
    }

    /**
     * @return The size of the interval.
     */
    public int size() {
        return (to - from) + 1;
    }

    public int getFrom() {
        return from;
    }

    public int getTo() {
        return to;
    }
}

