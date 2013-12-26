
package uzh.tomdb.db.operations.engines;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import uzh.tomdb.db.operations.Select;
import uzh.tomdb.db.operations.helpers.AndCondition;
import uzh.tomdb.db.operations.helpers.Conditions;
import uzh.tomdb.db.operations.helpers.Row;
import uzh.tomdb.db.operations.helpers.WhereCondition;
import uzh.tomdb.parser.MalformedSQLQuery;
import uzh.tomdb.parser.Tokens;

/**
 * 
 * Handles the conditions filtering the rows that do not match the conditions.
 * 
 * @author Francesco Luminati
 */
public class ConditionsHandler implements Handler{
//	private final Logger logger = LoggerFactory.getLogger(ConditionsHandler.class);
	/**
	 * Select object to get the MetaData information and to return the result rows.
	 */
	private Select select;
	/**
	 * To handle asynchronous DHT operations.
	 */
	private Set<String> futureManager = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
	/**
	 * Unparsed conditions.
	 */
	private List<WhereCondition> conditions;
	/**
	 * AND condition is an object containing a list of AND.
	 */
	private AndCondition andCond = new AndCondition();
	/**
	 * OR condition is a list, can contain OR conditions and AND objects.
	 */
	private List<Conditions> orCond = new ArrayList<>();
	/**
	 * Columns of the result row.
	 */
	private Map<String, Integer> resultRowCols;
	
	/**
	 * The constructor sets also the columns in the ResultSet.
	 * 
	 * @param select object to give the results back to the select.
	 */
	public ConditionsHandler(Select select) throws MalformedSQLQuery {
		this.select = select;
		this.conditions = select.getWhereConditions();
		
		//Set columns in result set and resultRowCols!!
		if (select.isAllColumns()) {
			select.setResultSetColumns(select.getTc().getColumns());
			resultRowCols = select.getTc().getColumns();
		} else {
			Map<String, Integer> cols = new LinkedHashMap<>();
			for (int i = 0; i < select.getColumns().size(); i++) {
				cols.put(select.getColumns().get(i), i);
			}
			select.setResultSetColumns(cols);
			resultRowCols = cols;
		}
		
		init();
	}
	
	/**
	 * Used for the Joins
	 * 
	 * @param select
	 * @param conditions
	 */
	public ConditionsHandler(Select select, List<WhereCondition> conditions) throws MalformedSQLQuery {
		this.select = select;
		this.conditions = conditions;
		
		//Set columns in resultRowCols!!
		if (!select.isAllColumns()) {
			Map<String, Integer> cols = new LinkedHashMap<>();
			for (int i = 0; i < select.getColumns().size(); i++) {
				cols.put(select.getColumns().get(i), i);
			}
			resultRowCols = cols;
		}
		
		init();
	}
	
	/**
	 * Internal Parser to separate the conditions in AND and OR conditions.
	 */
	private void init() throws MalformedSQLQuery {
		
		Iterator<WhereCondition> it = conditions.iterator();
		while (it.hasNext()) {
			WhereCondition cond = it.next();
			switch (cond.getBoolOperator()) {
			case "":
				if (!it.hasNext()) {
					orCond.add(cond);
				} else {
					throw new MalformedSQLQuery("Empty boolean operator!");
				}
				break;
			case Tokens.AND:
				if (it.hasNext()) {
					andCond.addCondition(cond);
					recParseAndCond(it.next(), it);
				} else {
					throw new MalformedSQLQuery("AND must have two conditions!");
				}
				
				break;
			case Tokens.OR:
				if (it.hasNext()) {
					orCond.add(cond);
				} else {
					throw new MalformedSQLQuery("OR must have two conditions!");
				}
				break;		
			}
		}
		
	}
	
	/**
	 * For the AND conditions a recursive function is used to get all the consecutive AND conditions and save them in a single AndCondition object.
	 * 
	 * @param condition
	 * @param iterator
	 */
	private void recParseAndCond(WhereCondition cond, Iterator<WhereCondition> it) throws MalformedSQLQuery {

		switch (cond.getBoolOperator()) {
		case "":
			andCond.addCondition(cond);
			break;
		case Tokens.OR:
			if (it.hasNext()) {
				andCond.addCondition(cond);
				orCond.add(andCond);
				andCond = new AndCondition();
			} else {
				throw new MalformedSQLQuery("OR must have two conditions!");
			}
			break;
		case Tokens.AND:
			if (it.hasNext()) {
				andCond.addCondition(cond);
				recParseAndCond(it.next(), it);
			} else {
				throw new MalformedSQLQuery("AND must have two conditions!");
			}
			break;
		}

	}
	
	/**
	 * The rows asynchronous coming from a tablescan or indexscan are analyzed against the conditions.
	 * Only rows respecting all the conditions are added to the ResultSet.
	 * The FutureManager is used to decide when all the asynchronous DHT operations are terminated; in this case the BlockingQueue in ResultSet is stopped.
	 * 
	 * @param Data Map of the DHT
	 * @param future.toString()
	 */
	public void filterRows(Map<Number160, Data> dataMap, String future) throws ClassNotFoundException, IOException, NumberFormatException, MalformedSQLQuery {
		
		for (Map.Entry<Number160, Data> entry : dataMap.entrySet()) {
			
			Row row = (Row) entry.getValue().getObject();
			
			/**
			 * Skip empty rows of DELETE operations.
			 */
			if (row.getRowID() >= 0) {
				if (andCond.getConditions().size() == 0 && orCond.size() == 0) {
					select.addToResultSet(filterColumns(row));
				} else if (orCond.size() > 0 && andCond.getConditions().size() > 0) { //if both set is the case of () OR () AND () ...
					if (checkOrConditions(row) || checkAndConditions(andCond, row)) {
						select.addToResultSet(filterColumns(row));
					}
				} else if (orCond.size() > 0) {
					if (checkOrConditions(row)) {
						select.addToResultSet(filterColumns(row));
					}
				} else if (andCond.getConditions().size() > 0) {
					if (checkAndConditions(andCond, row)) {
						select.addToResultSet(filterColumns(row));
					}
				}
			}	
			
		}
		
		removeFromFutureManager(future);
		/**
		 * Poison the blocking queue.
		 */
		if (futureManager.isEmpty()) {
			select.addToResultSet(new Row(-1));
		}
	}
	
	/**
	 * The rows coming from a join are analyzed against the conditions.
	 * Only rows respecting all the conditions are added to the ResultSet.
	 * 
	 * @param row
	 */
	public void filterJoinedRow(Row row) throws NumberFormatException, MalformedSQLQuery {
		
		if (andCond.getConditions().size() == 0 && orCond.size() == 0) {
			select.addToResultSet(filterColumns(row));
		} else if (orCond.size() > 0 && andCond.getConditions().size() > 0) { //if both set is the case of () OR () AND () ...
			if (checkOrConditions(row) || checkAndConditions(andCond, row)) {
				select.addToResultSet(filterColumns(row));
			}
		} else if (orCond.size() > 0) {
			if (checkOrConditions(row)) {
				select.addToResultSet(filterColumns(row));
			}
		} else if (andCond.getConditions().size() > 0) {
			if (checkAndConditions(andCond, row)) {
				select.addToResultSet(filterColumns(row));
			}
		}
		
	}
	
	/**
	 * Check the validity of the OR conditions for the given row.
	 * 
	 * @param row
	 */
	private boolean checkOrConditions(Row row) throws NumberFormatException, MalformedSQLQuery {
		int trueNum = 0;
		for (Conditions cond: orCond) {
			if (cond.getType().equals(Tokens.AND)) {
				AndCondition aCond = (AndCondition) cond;
				if (checkAndConditions(aCond, row)) {
					trueNum++;
				}
			} else {
				WhereCondition wCond = (WhereCondition) cond;
				if (checkCondition(wCond, row)) {
					trueNum++;
				}
			}
		}
		if (trueNum > 0) {
			return true;
		}
		return false;
	}
	
	/**
	 * Check the validity of the AND conditions for the given row.
	 * 
	 * @param and condition
	 * @param row
	 */
	private boolean checkAndConditions(AndCondition aCond, Row row) throws NumberFormatException, MalformedSQLQuery {
		int trueNum = 0;
		for (Conditions condition: aCond.getConditions()) {
			WhereCondition cond = (WhereCondition) condition;
			if(checkCondition(cond, row)) {
				trueNum++;
			}
		}
		if (trueNum == aCond.getConditions().size()) {
			return true;
		}
		return false;
	}
	
	/**
	 * Check the condition itself (=, <, >, <=, >=, !=).
	 * 
	 * @param condition
	 * @param row
	 */
	private boolean checkCondition(WhereCondition condition, Row row) throws NumberFormatException, MalformedSQLQuery {
		
		if (condition.getOperator().equals(Tokens.EQUAL)) {
			if (row.getCol(condition.getColumn()).equals(condition.getValue())) {
				return true;
			} else {
				return false;
			}
		}
		
		int colVal = Integer.parseInt(row.getCol(condition.getColumn()));
		int val = Integer.parseInt(condition.getValue());
		
		switch (condition.getOperator()) {
		case Tokens.GREATER:
			if (colVal > val) {
				return true;
			}
			break;
		case Tokens.GREATEREQUAL:
			if (colVal >= val) {
				return true;
			}
			break;
		case Tokens.LESS:
			if (colVal < val) {
				return true;
			}
			break;
		case Tokens.LESSEQUAL:
			if (colVal <= val) {
				return true;
			}
			break;
		case Tokens.NOTEQUAL:
			if (colVal != val) {
				return true;
			}
			break;
		}
		return false;
	}
	
	/**
	 * Add to the ResultSet only the columns interested by the SELECT.
	 * 
	 * @param row
	 */
	private Row filterColumns(Row row) {

		if (select.isAllColumns()) {
			return row;
		} else {	
			String tabName = "";
			if (select.getTabName() == null) {
				tabName = "join";
			} else {
				tabName = select.getTabName();
			}
			Row tmpRow = new Row(tabName, row.getRowID(), resultRowCols);
			for (int i = 0; i < select.getColumns().size(); i++) {
				tmpRow.setCol(i, row.getCol(select.getColumns().get(i)));
			}
			return tmpRow;
		}

	}
	
	@Override
	public void addToFutureManager(String future) {
		futureManager.add(future);
	}
	@Override
	public void removeFromFutureManager(String future) {
		futureManager.remove(future);
	}
	
	public Set<String> getFutureManager() {
		return futureManager;
	}
	
	public List<Conditions> getAndCond() {
		return andCond.getConditions();
	}

	public List<Conditions> getOrCond() {
		return orCond;
	}
	
}
