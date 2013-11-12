
package uzh.tomdb.parser;

import java.util.ArrayList;
import java.util.List;

import uzh.tomdb.api.ResultSet;
import uzh.tomdb.db.operations.CreateTable;
import uzh.tomdb.db.operations.Delete;
import uzh.tomdb.db.operations.Insert;
import uzh.tomdb.db.operations.Operations;
import uzh.tomdb.db.operations.Select;
import uzh.tomdb.db.operations.Update;
import uzh.tomdb.db.operations.engines.QueryEngine;
import uzh.tomdb.db.operations.helpers.SetCondition;
import uzh.tomdb.db.operations.helpers.WhereCondition;
import uzh.tomdb.p2p.DBPeer;

/**
 *
 * @author Francesco Luminati
 */
public class SQLParser {
    
    private Tokenizer tokens;
    private List<Operations> bufferedQueries = new ArrayList<>();
    
    public void parse(String sql) throws MalformedSQLQuery, Exception {

        tokens = new Tokenizer(sql);
        
        String token = tokens.next();
        switch (token) {
            case Tokens.SELECT:
                throw new Exception("API ERROR, use executeQuery() instead for SELECT statements!");
            case Tokens.INSERT:
                insert(tokens);
                break;
            case Tokens.UPDATE:
                update(tokens);
                break;
            case Tokens.CREATE:
                create(tokens);
                break;
            case Tokens.DELETE:
                delete(tokens);
                break;
            case Tokens.FETCH:
                fetch(tokens);
                break;
            default:
                throw new MalformedSQLQuery(tokens);
        }

    }

	public ResultSet parseQuery(String sql) throws MalformedSQLQuery, Exception {
        
        tokens = new Tokenizer(sql);
        
        String token = tokens.next();
        switch (token) {
            case Tokens.SELECT:
                return select(tokens);
            default: 
                throw new Exception("API ERROR, use execute() instead for non SELECT operations!");
        }
        
    }
    
    public ResultSet parseQueryFetch(String sql) throws MalformedSQLQuery, Exception {
    	DBPeer.fetchTableRows();
    	return parseQuery(sql);
    }

    private void create(Tokenizer tokens) throws MalformedSQLQuery {

        if (tokens.next().equals(Tokens.TABLE)) {
            List<String> columns = new ArrayList<>();

            String tabName = tokens.next();

            getParenthesis(tokens, columns);

            if (tokens.hasNext()) {
                if (tokens.next().equals(Tokens.OPTIONS)) {
                    List<String> options = new ArrayList<>();
                    getParenthesis(tokens, options);
                    CreateTable createTable = new CreateTable(tabName, columns, options);
                    bufferedQueries.add(createTable);
                } else {
                    throw new MalformedSQLQuery(tokens);
                }

            } else {
                CreateTable createTable = new CreateTable(tabName, columns);
                bufferedQueries.add(createTable);
            }

        } else {
            throw new MalformedSQLQuery(tokens);
        }

    }
    
    private void insert(Tokenizer tokens) throws MalformedSQLQuery {

        if (tokens.next().equals(Tokens.INTO)) {
            List<String> values = new ArrayList<>();
            List<String> columns = new ArrayList<>();

            String tabName = tokens.next();
            String token = tokens.next();
            
            if (token.equals(Tokens.POPEN)) {
                tokens.previous();
                getParenthesis(tokens, columns);
                token = tokens.next();
            }

            if (token.equals(Tokens.VALUES)) {
                getParenthesis(tokens, values);
            } else {
                throw new MalformedSQLQuery(tokens);
            }

            if (!tokens.hasNext()) {
                if (!columns.isEmpty()) {
                    Insert insert = new Insert(tabName, values, columns);
                    bufferedQueries.add(insert);
                } else {
                    Insert insert = new Insert(tabName, values);
                    bufferedQueries.add(insert);
                }
            } else {
                throw new MalformedSQLQuery(tokens);
            }

        } else {
            throw new MalformedSQLQuery(tokens);
        }

    }

    private ResultSet select(Tokenizer tokens) throws MalformedSQLQuery {
        boolean allColumns = false; 
        List<String> columns = new ArrayList<>();
        List<WhereCondition> whereConditions = new ArrayList<>();
        String scanType = "tablescan";
        String tabNames;
        
    	OUTER:
    	while (tokens.hasNext()) {
            
    		String token = tokens.next();
            
            switch(token) {
            case Tokens.ASTERISK:
            	allColumns = true;
            	break OUTER;
            case Tokens.FROM:
            	tokens.previous();
            	break OUTER;
            case Tokens.COMMA:
            	break;
            default:
            	columns.add(token);
            }

        }
        
        
        if (tokens.next().equals(Tokens.FROM)) {
        	tabNames = tokens.next();
        } else {
            throw new MalformedSQLQuery(tokens);
        }
        
        if (tokens.hasNext()) {
            if (tokens.next().equals(Tokens.WHERE)) {
                 getWhereConditions(tokens, whereConditions);
            } else {
                throw new MalformedSQLQuery(tokens);
            }
        }
        
        if (tokens.hasNext()) {
            if (tokens.next().equals(Tokens.OPTIONS)) {
            	List<String> tmpList = new ArrayList<>();
            	getParenthesis(tokens, tmpList);
            	scanType = tmpList.get(0);
            }
        }

        Select select = new Select(allColumns, tabNames, columns, whereConditions, scanType);
        select.init();
        return select.getResultSet();

    }

    private void update(Tokenizer tokens) throws MalformedSQLQuery {
        String tabName;
        List<SetCondition> setConditions = new ArrayList<>();
        List<WhereCondition> whereConditions = new ArrayList<>();
        
        tabName = tokens.next();
        
        if (tokens.next().equals(Tokens.SET)) {
            getSetConditions(tokens, setConditions);
        } else {
            throw new MalformedSQLQuery(tokens);
        }
        
        if (tokens.next().equals(Tokens.WHERE)) {
            getWhereConditions(tokens, whereConditions);
        } else {
            throw new MalformedSQLQuery(tokens);
        }
        
        Update update = new Update(tabName, setConditions, whereConditions);
        bufferedQueries.add(update);
    }

    private void delete(Tokenizer tokens) throws MalformedSQLQuery {
        String tabName;
        List<WhereCondition> whereConditions = new ArrayList<>();

        if (tokens.next().equals(Tokens.FROM)) {
            tabName = tokens.next();
        } else {
            throw new MalformedSQLQuery(tokens);
        }

        if (tokens.next().equals(Tokens.WHERE)) {
            getWhereConditions(tokens, whereConditions);
        } else {
            throw new MalformedSQLQuery(tokens);
        }

        Delete delete = new Delete(tabName, whereConditions);
        bufferedQueries.add(delete);   
    }
    
    private void fetch(Tokenizer tokens) throws MalformedSQLQuery {
		if (tokens.next().equals(Tokens.METADATA)) {
			DBPeer.fetchTableColumns();
			DBPeer.fetchTableRows();
			DBPeer.fetchTableIndexes();
		} else {
			throw new MalformedSQLQuery(tokens);
		}
	}
    
    private void getParenthesis(Tokenizer tokens, List<String> list) throws MalformedSQLQuery {
        String token = tokens.next();
        if (token.equals(Tokens.POPEN)) {
            while (tokens.hasNext()) {
                token = tokens.next();
                
                if (token.equals(Tokens.PCLOSE)) {
                    break;
                }
                
                if (token.equals(Tokens.QUOTE)) {
                    list.add(getQuote(tokens));
                    token = tokens.next();
                    if (token.equals(Tokens.COMMA) || token.equals(Tokens.PCLOSE)) {
                    } else {
                        throw new MalformedSQLQuery(tokens);
                    }
                } else if (!token.equals(Tokens.COMMA)) {
                    list.add(token);
                }
                
                
            }
            if (!token.equals(Tokens.PCLOSE)) {
                throw new MalformedSQLQuery(tokens);
            }
        } else {
            throw new MalformedSQLQuery(tokens);
        }
    }
    
    private String getQuote(Tokenizer tokens) throws MalformedSQLQuery {
        String val = "";
        
        if (tokens.thisElement().equals(Tokens.QUOTE)) {
            String token = "";
            while (tokens.hasNext()) {
                token = tokens.next();

                if (token.equals(Tokens.QUOTE)) {
                    break;
                }

                if (!val.isEmpty()) {
                    val += " " + token;
                } else {
                    val += token;
                }
            }
            if (!token.equals(Tokens.QUOTE)) {
                throw new MalformedSQLQuery(tokens);
            }
        } else {
            throw new MalformedSQLQuery(tokens);
        }
        return val;
    }

    private void getWhereConditions(Tokenizer tokens, List<WhereCondition> conditions) throws MalformedSQLQuery {
        WhereCondition condition = new WhereCondition();
        String token = null;
        OUTER:
        while(tokens.hasNext()) {
            token = tokens.next();
            switch (token) {
                case Tokens.AND:
                case Tokens.OR:
                    if (condition.isSet()) {
                    	condition.setBoolOperator(token);
                        conditions.add(condition);
                        condition = new WhereCondition();
                    } else {
                        throw new MalformedSQLQuery(tokens, "WhereCondition ERROR: not set!");
                    }
                    break;
                case Tokens.EQUAL:
                case Tokens.GREATER:
                case Tokens.LESS:
                case Tokens.NOT:
                    if (tokens.next().equals(Tokens.EQUAL)) {
                        switch(token) {
                            case "GREATER":
                                condition.setOperator(Tokens.GREATEREQUAL);
                                break;
                            case "LESS":
                                condition.setOperator(Tokens.LESSEQUAL);
                                break;
                            case "NOT":
                                condition.setOperator(Tokens.NOTEQUAL);
                        }
                    } else {
                        tokens.previous();
                        condition.setOperator(token);
                    }
                    break;
//                case Tokens.QUOTE:
//                    if (tokens.hasNext()) {
//                            operation.setValue(tokens.next());
//                        } else {
//                            throw new MalformedSQLQuery(tokens);
//                        }
//                        if (tokens.hasNext() && tokens.next().equals(Tokens.QUOTE)) {
//                        } else {
//                            throw new MalformedSQLQuery(tokens);
//                        }
//                    break;
                case Tokens.OPTIONS:
                	tokens.previous();
                	break OUTER;
                default:
                    condition.setColOrVal(token);
                    break;
            }
        }
        if (condition.isSet()) {
        	conditions.add(condition);
        } else {
            throw new MalformedSQLQuery(tokens, "WhereCondition ERROR: not set!");
        }
       
    }
    
    private void getSetConditions(Tokenizer tokens, List<SetCondition> conditions) throws MalformedSQLQuery {
        SetCondition condition = new SetCondition();
        String token;
        OUTER:
        while (tokens.hasNext()) {
            token = tokens.next();
            switch (token) {
                case Tokens.WHERE:
                    if (condition.isSet()) {
                        conditions.add(condition);
                    } else {
                        throw new MalformedSQLQuery(tokens, "SetCondition ERROR: not set!");
                    }
                    tokens.previous();
                    break OUTER;
                case Tokens.COMMA:
                    if (condition.isSet()) {
                        conditions.add(condition);
                        condition = new SetCondition();
                    } else {
                        throw new MalformedSQLQuery(tokens, "SetCondition ERROR: not set!");
                    }
                    break;
                case Tokens.QUOTE:
                    if (tokens.hasNext()) {
                        condition.setValue(tokens.next());
                    } else {
                        throw new MalformedSQLQuery(tokens);
                    }
                    if (tokens.hasNext() && tokens.next().equals(Tokens.QUOTE)) {
                    } else {
                        throw new MalformedSQLQuery(tokens);
                    }
                    break;
                case Tokens.EQUAL:
                    break;
                default:
                    condition.setColOrVal(token);
                    break;
            }
           
        }
    }

    public void start() {
        QueryEngine eng = new QueryEngine(bufferedQueries);
        eng.start();
        bufferedQueries.clear();
    }

}
