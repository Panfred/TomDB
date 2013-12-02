
package uzh.tomdb.api;

/**
 * 
 * Connection (like JDBC)
 * 
 * @author Francesco Luminati
 */
public class Connection {
    
	/**
	 * To create a Statement
	 * 
	 * @return statement
	 */
    public Statement createStatement() {
        return new Statement();
    }
    
}
