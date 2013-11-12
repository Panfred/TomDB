
package uzh.tomdb.db.operations.engines;


import java.io.IOException;

import uzh.tomdb.db.operations.Select;
import uzh.tomdb.db.operations.helpers.ConditionsHandler;
import uzh.tomdb.parser.MalformedSQLQuery;

/**
 * 
 * @author Francesco Luminati
 *
 */
public class QueryExecuter {
	private Select select;
	
	private ConditionsHandler condHandler;
	
	public QueryExecuter(Select select) throws MalformedSQLQuery, ClassNotFoundException, IOException {
		this.select = select;
		init();
	}
	
	private void init() throws MalformedSQLQuery, ClassNotFoundException, IOException {
		condHandler = new ConditionsHandler(select);

		switch (select.getScanType()) {
		case "tablescan":
			new TableScan(select, condHandler).start();
			break;
		case "indexscan":
			new IndexScan(select, condHandler).start();
			break;
		}
		
	}

	
}
