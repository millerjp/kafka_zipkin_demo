package thinkport.meetup;

import brave.internal.propagation.CorrelationFieldScopeDecorator;
import brave.propagation.CurrentTraceContext;
import org.apache.logging.log4j.ThreadContext;

public final class ThreadContextScopeDecorator extends CorrelationFieldScopeDecorator {

	public static CurrentTraceContext.ScopeDecorator create() {
		return new ThreadContextScopeDecorator();
	}

	@Override
	protected String get(String key) {
		return ThreadContext.get(key);
	}

	@Override
	protected void put(String key, String value) {
		ThreadContext.put(key, value);
	}

	@Override
	protected void remove(String key) {
		ThreadContext.remove(key);
	}

	ThreadContextScopeDecorator() {
	}

}
