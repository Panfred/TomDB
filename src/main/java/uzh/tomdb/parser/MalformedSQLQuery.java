
package uzh.tomdb.parser;

/**
 *
 *	Exception for the SQL queries.
 *	Utilize Tokenizer to show in which position there is a problem in the query.
 *
 * @author Francesco Luminati
 */
public class MalformedSQLQuery extends Exception {
  
	private static final long serialVersionUID = 1L;
	
	public MalformedSQLQuery(Tokenizer tokens) {
        super("SQL Query Error: the SQL Query [" +
                tokens.SQLString() +
              "] has an error at [" +
                tokens.previous() + "]");
    }
    
    public MalformedSQLQuery(Tokenizer tokens, String message) {
        super("SQL Query Error: the SQL Query [" +
                tokens.SQLString() +
              "] has an error at [" +
                tokens.previous() + "] ADDITIONAL MESSAGE: "+ message);
    }
    
    public MalformedSQLQuery(String message) {
        super("SQL Query Error: MESSAGE: " + message);
    }

}
