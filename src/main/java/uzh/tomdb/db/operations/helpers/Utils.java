
package uzh.tomdb.db.operations.helpers;

import java.util.ArrayList;
import java.util.List;

import uzh.tomdb.db.indexes.DSTBlock;

/**
 * 
 * Utility class for the calculation of the table and DST blocks.
 * 
 * @author Francesco Luminati
 */
public class Utils {
    
	/**
     * Give the last Block for the given rowId.
     * 
     * @param rowId
     * @param blockCapacity
     * @param table name
     * @return last block
     */
    public static Block getLastBlock(int rowId, int blockCapacity, String table) {
        Block block;
       
        if (rowId % blockCapacity != 0) {
            if (rowId / blockCapacity > 0) {
                int lower = rowId - (rowId % blockCapacity);
                block = new Block(lower + 1, lower + blockCapacity, table);
            } else {
                block = new Block(1, blockCapacity, table);
            }
           
        } else {
            block = new Block(rowId - blockCapacity + 1, rowId, table);
        }
        
        return block;  
    }
    
    /**
     * Give the Blocks for a range.
     * 
     * @param from
     * @param to
     * @param numRows
     * @param blockCapacity
     * @param table name
     * @return list of blocks
     */
    public static List<Block> getBlocks(int from, int to, int numRows, int blockCapacity, String table) {
        List<Block> retVal = new ArrayList<>();
        int blocks;
        
        if (numRows % blockCapacity != 0) {
            blocks = numRows / blockCapacity + 1;
        } else {
            blocks = numRows / blockCapacity;
        }
        
        for (int i = 0; i < blocks; i++) {
            int lower = i * blockCapacity + 1;
            int upper = i * blockCapacity + blockCapacity;
            boolean added = false;

            if (lower <= from && upper >= from) {
                if (!added) {
                    retVal.add(new Block(lower, upper, table));
                    added = true;
                }
            }
            if (lower <= to && upper >= to) {
                if (!added) {
                    retVal.add(new Block(lower, upper, table));
                    added = true;
                }
            }
            if (lower > from && upper < to) {
                if (!added) {
                    retVal.add(new Block(lower, upper, table));
                }
            }

        }
        return retVal;
    }
    
    public static int getDSTHeight(int upper) {
    	return (int) (Math.log(upper) + 1);
    }
    
    /**
     * Splits a range into Blocks for the DST.
     * 
     * @param from
     *            Range from
     * @param to
     *            Range to
     * @param upper
     *            Upper bound
     * @return The intervals for a segment
     */
    public static List<DSTBlock> splitRange(int from, int to, int upper, String column) {
        List<DSTBlock> retVal = new ArrayList<>();
        splitRange(from, to, 0, upper, getDSTHeight(upper), retVal, column);
        return retVal;
    }
    
    /**
     * Splits a range into Blocks for the DST (recursive).
     * 
     * @param s
     *            Range from
     * @param t
     *            Range to
     * @param lower
     *            Lower bound
     * @param upper
     *            Upper bound
     * @param maxDepth
     *            Hierarchy level
     * @param retVal
     *            Result collection
     * @param column
     * 			  Column name
     */
    private static void splitRange(int s, int t, int lower, int upper, int maxDepth, final List<DSTBlock> retVal, String column) {
        if (s <= lower && upper <= t || maxDepth == 0) {
            retVal.add(new DSTBlock(lower, upper, column));
            return;
        }
        int mid = (lower + upper) / 2;
        if (s <= mid) {
            splitRange(s, t, lower, mid, maxDepth - 1, retVal, column);
        }
        if (t > mid) {
            splitRange(s, t, mid + 1, upper, maxDepth - 1, retVal, column);
        }
    }
    
}
