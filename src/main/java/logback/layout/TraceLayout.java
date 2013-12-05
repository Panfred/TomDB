package logback.layout;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.CoreConstants;
import ch.qos.logback.core.LayoutBase;

public class TraceLayout extends LayoutBase<ILoggingEvent> {
	
	public String doLayout(ILoggingEvent event) {
		StringBuffer sbuf = new StringBuffer(128);
		int added = 0;
		
		sbuf.append(event.getTimeStamp() - event.getLoggerContextVO().getBirthTime()); //time from beginning of program
	    sbuf.append(",");
	    added++;
//	    sbuf.append(event.getThreadName());
//	    sbuf.append(",");
//	    added++;
	    sbuf.append(event.getLoggerName());
	    sbuf.append(",");
	    added++;
	    sbuf.append(event.getMessage());
	    added++;
	    
    	for (Object obj: event.getArgumentArray()) {
	    	sbuf.append(",");
	    	sbuf.append(obj);
	    	added++;
	    }
	    
    	if (added <= 8) {
    		sbuf.append(",null");
    	}
    	
	    sbuf.append(CoreConstants.LINE_SEPARATOR);
	    
	    return sbuf.toString();
	}
	
}
