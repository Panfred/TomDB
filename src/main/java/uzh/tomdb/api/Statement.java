
package uzh.tomdb.api;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import uzh.tomdb.parser.MalformedSQLQuery;
import uzh.tomdb.parser.SQLParser;

/**
 *
 * DataBase API (like JDBC)
 * 
 * @author Francesco Luminati
 */
public class Statement {
    private final Logger logger = LoggerFactory.getLogger(Statement.class);
    private SQLParser parser = new SQLParser();
    
    /**
     * Only used for TRACE logging of the experiments.
     */
    public static int experiment = 0;
    
    /**
     * Execute SQL query.
     * 
     * Used for CREATE TABLE, INSERT, UPDATE, DELETE.
     * 
     * The queries are buffered and executed only when START is called.
     * 
     * @param sql 
     *          the SQL Query
     */
    public boolean execute(String sql) {
        try {
            parser.parse(sql);
        } catch (MalformedSQLQuery e) {
            logger.warn("SQL Query Error", e);
            return false;
        } catch (Exception e) {
            logger.error("Statement Error", e);
            return false;
        }
        return true;
    }
    
    /**
     * Execute SQL query.
     * 
     * Used for SELECT statements.
     * 
     * The query is executed immediately and returns a ResultSet.
     * 
     * @param sql 
     *          the SQL Query
     * @return ResultSet
     */
    public ResultSet executeQuery(String sql) {
        try {
            return parser.parseQuery(sql);
        } catch (MalformedSQLQuery e) {
            logger.warn("SQL Query Error", e);
        } catch (Exception e) {
            logger.error("Statement Error", e);
        }
        return null;
    }
    
    /**
     * Same as above for experiments
     * 
     * @param sql
     * @param exp
     * @return
     */
    public ResultSet executeQuery(String sql, int exp) {
    	experiment = exp;
        try {
            return parser.parseQuery(sql);
        } catch (MalformedSQLQuery e) {
            logger.warn("SQL Query Error", e);
        } catch (Exception e) {
            logger.error("Statement Error", e);
        }
        return null;
    }
    
    /**
     * Execute SQL query updating the MetaData.
     * 
     * Used for SELECT statements.
     * 
     * The query is executed immediately and returns a ResultSet.
     * 
     * @param sql 
     *          the SQL Query
     * @return ResultSet
     */
    public ResultSet executeQueryFetch(String sql) {
        try {
            return parser.parseQuery(sql);
        } catch (MalformedSQLQuery e) {
            logger.warn("SQL Query Error", e);
        } catch (Exception e) {
            logger.error("Statement Error", e);
        }
        return null;
    }
    
    /**
     * Execute the buffered queries.
     */
    public void start() {
        parser.start();
    }
    
    /**
     * Same as above for experiments
     * 
     * @param exp
     */
    public void start(int exp) {
    	experiment = exp;
        parser.start();
    }
    
}