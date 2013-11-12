package uzh.tomdb.db.operations.helpers;

import java.util.ArrayList;
import java.util.List;

import uzh.tomdb.parser.Tokens;

public class AndCondition implements Conditions{
	private List<Conditions> conditions = new ArrayList<>();

	public List<Conditions> getConditions() {
		return conditions;
	}
	
	public void addCondition(Conditions condition) {
		conditions.add(condition);
	}
	
	@Override
	public String getType() {
		return Tokens.AND;
	}
}
