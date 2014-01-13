
package uzh.tomdb.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;

/**
 *
 * Helper class to divide a String in single tokens.
 * Implements the ListIterator interface.
 *
 * @author Francesco Luminati
 */
public class Tokenizer implements ListIterator<String>{
    private int index = -1;
    /**
     * The string to tokenize.
     */
    private String sql;
    /**
     * The list of tokens derived form the string.
     */
    private List<String> tokens = new ArrayList<>();
    private ListIterator<String> iterator;
    
    public Tokenizer(String sql) {
        this.sql = sql;
        init();
        iterator = tokens.listIterator();
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
     }

    @Override
    public String next() {
        index++;
        return iterator.next();
    }

    @Override
    public void remove() {
        iterator.remove();
    }

    @Override
    public boolean hasPrevious() {
        return iterator.hasPrevious();
    }

    @Override
    public String previous() {
        index--;
        return iterator.previous();
    }
   
    public String getPrevious(int x) {      
        return tokens.get(index-x);
    }

    @Override
    public int nextIndex() {
        return iterator.nextIndex();
    }

    @Override
    public int previousIndex() {
        return iterator.previousIndex();
    }

    @Override
    public void set(String e) {
        iterator.set(e);
    }

    @Override
    public void add(String e) {
        iterator.add(e);
    }
    
    @Override
    public String toString() {
        return tokens.toString();
    }
    
    public String thisElement() {
        return tokens.get(index);
    }
    
    public String SQLString() {
        return sql;
    }
    
    /**
     * The tokenization is done here using regex delimiter and the String.slit(delimiter) function.
     * The switch decide if it is a token or a string.
     */
    private void init() {
        String delimiters = " |"; //space
        delimiters += "((?<=,)|(?=,))|"; //comma
        delimiters += "((?<=')|(?='))|"; //single quote
        delimiters += "((?<=\\()|(?=\\())|"; //open parenthesis
        delimiters += "((?<=\\))|(?=\\)))|"; //close parenthesis
        delimiters += "((?<=\\=)|(?=\\=))|"; //equal
        delimiters += "((?<=\\<)|(?=\\<))|"; //minor
        delimiters += "((?<=\\>)|(?=\\>))|"; //major
        delimiters += "((?<=\\!)|(?=\\!))"; //not
        
        String[] spl = sql.split(delimiters);
        
        for(int i = 0; i<spl.length; i++) {
        	
            switch(spl[i].toLowerCase(Locale.ENGLISH)) {
                case " ":
                    break;
                case ",":
                    tokens.add(Tokens.COMMA);
                    break;
                case "'":
                    tokens.add(Tokens.QUOTE);
                    break;
                case "(":
                    tokens.add(Tokens.POPEN);
                    break;
                case ")":
                    tokens.add(Tokens.PCLOSE);
                    break;
                case "*":
                    tokens.add(Tokens.ASTERISK);
                    break;
                case "=":
                    tokens.add(Tokens.EQUAL);
                    break;
                case "<":
                    tokens.add(Tokens.LESS);
                    break;
                case ">":
                    tokens.add(Tokens.GREATER);
                    break;
                case "!":
                    tokens.add(Tokens.NOT);
                    break;
                case "select":
                    tokens.add(Tokens.SELECT);
                    break;
                case "from":
                    tokens.add(Tokens.FROM);
                    break;
                case "where":
                    tokens.add(Tokens.WHERE);
                    break;
                case "and":
                    tokens.add(Tokens.AND);
                    break;
                case "or":
                    tokens.add(Tokens.OR);
                    break;
                case "insert":
                    tokens.add(Tokens.INSERT);
                    break;
                case "into":
                    tokens.add(Tokens.INTO);
                    break;
                case "values":
                    tokens.add(Tokens.VALUES);
                    break;
                case "update":
                    tokens.add(Tokens.UPDATE);
                    break;
                case "set":
                    tokens.add(Tokens.SET);
                    break;
                case "delete":
                    tokens.add(Tokens.DELETE);
                    break;
                case "create":
                    tokens.add(Tokens.CREATE);
                    break;
                case "table":
                    tokens.add(Tokens.TABLE);
                    break;
                case "options":
                    tokens.add(Tokens.OPTIONS);
                    break;
                case "fetch":
                    tokens.add(Tokens.FETCH);
                    break;
                case "metadata":
                    tokens.add(Tokens.METADATA);
                    break;
                case "":
                    break;
                default:
                    tokens.add(spl[i]);
            }    
        }
    }
}
