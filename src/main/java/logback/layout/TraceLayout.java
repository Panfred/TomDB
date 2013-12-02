package logback.layout;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.CoreConstants;
import ch.qos.logback.core.LayoutBase;

public class TraceLayout extends LayoutBase<ILoggingEvent> {

	public String doLayout(ILoggingEvent event) {
		StringBuffer sbuf = new StringBuffer(128);
		
		sbuf.append(event.getTimeStamp() - event.getLoggerContextVO().getBirthTime()); //time from beginning of program
	    sbuf.append(",");
	    sbuf.append(event.getThreadName());
	    sbuf.append(",");
	    sbuf.append(event.getLoggerName());
	    
	    String message = event.getMessage();
	    String[] splitted = message.split(" ");
	    
	    for (String str: splitted) {
	    	sbuf.append(",");
	    	sbuf.append(str);
	    }

    	for (Object obj: event.getArgumentArray()) {
	    	sbuf.append(",");
	    	sbuf.append(obj);
	    }
	    
	    sbuf.append(CoreConstants.LINE_SEPARATOR);
	    
	    return sbuf.toString();
	}
	
}
