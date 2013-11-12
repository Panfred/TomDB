package uzh.tomdb.db.operations.helpers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uzh.tomdb.db.operations.Select;
import uzh.tomdb.parser.MalformedSQLQuery;
import uzh.tomdb.parser.Tokens;

public class ConditionsHandler {
	private final Logger logger = LoggerFactory.getLogger(ConditionsHandler.class);
	private Select select;
	private Set<String> futureManager = new HashSet<>();
	private List<WhereCondition> conditions;
	private AndCondition andCond = new AndCondition();
	private List<Conditions> orCond = new ArrayList<>();
	
	public ConditionsHandler(Select select) throws MalformedSQLQuery {
		this.select = select;
		this.conditions = select.getWhereConditions();
		init();
	}
	
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
	
	public void filterRows(Map<Number160, Data> dataMap, String future) throws ClassNotFoundException, IOException, NumberFormatException, MalformedSQLQuery {
		
		for (Map.Entry<Number160, Data> entry : dataMap.entrySet()) {
			
			Row row = (Row) entry.getValue().getObject();
			
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
		
		removeFromFutureManager(future);
		/**
		 * Poison the blocking queue.
		 */
		if (futureManager.isEmpty()) {
			select.addToResultSet(new Row(-1));
		}
	}
	
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
	
	private boolean checkCondition(WhereCondition condition, Row row) throws NumberFormatException, MalformedSQLQuery {
		
		int colVal = Integer.parseInt(row.getCol(select.getTc().getColumnId(condition.getColumn())));
		int val = Integer.parseInt(condition.getValue());

		switch (condition.getOperator()) {
		case Tokens.EQUAL:
			if (colVal == val) {
				return true;
			}
			break;
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

	private Row filterColumns(Row row) {

		if (select.isAllColumns() || select.getColumns().size() == select.getTc().getNumOfCols()) {
			return row;
		} else {

			Row tmpRow = new Row(row.getRowID());
			for (int i = 0; i < select.getColumns().size(); i++) {
				try {
					tmpRow.setCol(i, row.getCol(select.getTc().getColumnId(select.getColumns().get(i))));
				} catch (MalformedSQLQuery e) {
					logger.error("filterColumns SQL error", e);
				}
			}
			return tmpRow;
	}

	}
	public void addToFutureManager(String future) {
		futureManager.add(future);
	}
	public void removeFromFutureManager(String future) {
		futureManager.remove(future);
	}

	public List<Conditions> getAndCond() {
		return andCond.getConditions();
	}

	public List<Conditions> getOrCond() {
		return orCond;
	}
	
}
