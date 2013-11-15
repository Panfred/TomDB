package uzh.tomdb.db.operations.engines;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import uzh.tomdb.db.TableColumns;
import uzh.tomdb.db.TableIndexes;
import uzh.tomdb.db.TableRows;
import uzh.tomdb.db.operations.Select;
import uzh.tomdb.db.operations.helpers.JoinCondition;
import uzh.tomdb.db.operations.helpers.Row;
import uzh.tomdb.db.operations.helpers.WhereCondition;
import uzh.tomdb.p2p.DBPeer;
import uzh.tomdb.parser.MalformedSQLQuery;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

public class JoinsHandler implements Handler{

	private Select select;
	private Set<String> futureManager = new HashSet<>();
	private List<WhereCondition> whereConditions;
	private JoinCondition joinCondition;
	private Map<String, Select> selects = new HashMap<>();
	private List<String> tabNames;
	private Map<String, Map<String, Integer>> invIndex = new HashMap<>();
	private Map<String, Map<Integer, Row>> rows = new HashMap<>();
	private Map<String, Integer> rowCols = new LinkedHashMap<>(); 
	private ConditionsHandler conditionsHandler;
	private Set<String> elaboratedRows = new HashSet<>();

	
	public JoinsHandler(Select select) throws MalformedSQLQuery {
		this.select = select;
		tabNames = select.getTabNames();
		init();
	}
	
	private void init() throws MalformedSQLQuery {
		
		whereConditions = select.getWhereConditions();
		if(whereConditions.size() > 0) {
			for (int i = 0; i < whereConditions.size(); i++) {
				WhereCondition cond = whereConditions.get(i);
				
				if (cond.isJoinCondition()) {
					JoinCondition jc = new JoinCondition();
					String[] one = dotSplitString(cond.getColumn());
					if (select.getTabNames().contains(one[0])) {
						jc.setTabOne(one[0]);
					} else {
						throw new MalformedSQLQuery("Wrong JOIN condition");
					}
					jc.setColumnOne(one[1]);
					String[] two = dotSplitString(cond.getValue());
					if (select.getTabNames().contains(two[0])) {
						jc.setTabTwo(two[0]);
					} else {
						throw new MalformedSQLQuery("Wrong JOIN condition");
					}
					jc.setColumnTwo(two[1]);
					joinCondition = jc;
					whereConditions.remove(i);
				} else {
					throw new MalformedSQLQuery("Wrong JOIN condition, just one join supported!");
				}
			}
		} else {
			throw new MalformedSQLQuery("No join condition!");
		}
		
		conditionsHandler = new ConditionsHandler(select, whereConditions);
	}
	
	public Collection<Select> getSelects() throws ClassNotFoundException, IOException {
		Map<Number160, Data> tabColumns = DBPeer.getTabColumns();
		Map<Number160, Data> tabRows = DBPeer.getTabRows();
		Map<Number160, Data> tabIndexes = DBPeer.getTabIndexes();
		
		for (String tabName: select.getTabNames()) {
			Number160 tabKey = Number160.createHash(tabName);
			TableColumns tc = (TableColumns) tabColumns.get(tabKey).getObject();
			TableRows tr = (TableRows) tabRows.get(tabKey).getObject();
			TableIndexes ti = (TableIndexes) tabIndexes.get(tabKey).getObject();
			selects.put(tabName, new Select(tabName, tr, ti, tc));
			
			invIndex.put(tabName, new HashMap<String, Integer>());
			rows.put(tabName, new HashMap<Integer, Row>());
		}
		
		//Set columns of joined row
		int counter = 0;
		for (Select select: selects.values()) {
			for (String name: select.getTc().getColumns().keySet()) {
				rowCols.put(name, counter++);
			}
		}
		//Set columns in result set!!!
		if (select.isAllColumns()) {
			select.setResultSetColumns(rowCols);
		} else {
			Map<String, Integer> cols = new LinkedHashMap<>();
			for (int i = 0; i < select.getColumns().size(); i++) {
				cols.put(select.getColumns().get(i), i);
			}
			select.setResultSetColumns(cols);
		}
		
		return selects.values();
	}
	
	@Override
	public synchronized void filterRows(Map<Number160, Data> dataMap, String future) throws ClassNotFoundException, IOException, MalformedSQLQuery {

		for (Map.Entry<Number160, Data> entry : dataMap.entrySet()) {
			
			Row row = (Row) entry.getValue().getObject();
			
			invIndex.get(row.getTabName()).put(row.getCol(joinCondition.getColumn(row.getTabName())), row.getRowID());
			rows.get(row.getTabName()).put(row.getRowID(), row);
			
		}

		checkMatches();
		
		removeFromFutureManager(future);
		/**
		 * Poison the blocking queue.
		 */
		if (futureManager.isEmpty()) {
			select.addToResultSet(new Row(-1));
		}
	}
	
	private void checkMatches() throws MalformedSQLQuery {
		
		String tabOne = tabNames.get(0);
		String tabTwo = tabNames.get(1);
		for (Map.Entry<String, Integer> val: invIndex.get(tabOne).entrySet()) {
			if (invIndex.get(tabTwo).containsKey(val.getKey())) {
				joinRows(rows.get(tabOne).get(val.getValue()), rows.get(tabTwo).get(invIndex.get(tabTwo).get(val.getKey())));
			}
		}
		
	}
	
	private void joinRows(Row rowOne, Row rowTwo) throws NumberFormatException, MalformedSQLQuery {
		
		List<String> row = new ArrayList<>();
		row.addAll(rowOne.getRow());
		row.addAll(rowTwo.getRow());
		Row joinRow = new Row("join", 0, row, rowCols);
		
		if(!elaboratedRows.contains(joinRow.toString())) {
			conditionsHandler.filterJoinedRow(joinRow);
			elaboratedRows.add(joinRow.toString());
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
	
	private String[] dotSplitString(String string) {
		return string.split("\\.");
	}
	
}
