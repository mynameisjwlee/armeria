/*
 * Copyright 2023 LINE Corporation
 *
 * LINE Corporation licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.linecorp.com.common.micrometer.tracing;

import static com.linecorp.com.internal.common.micrometer.tracing.TraceContextUtil.setTraceContext;
import static com.linecorp.com.internal.common.micrometer.tracing.TraceContextUtil.traceContext;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.armeria.common.RequestContext;
import com.linecorp.armeria.common.annotation.Nullable;
import com.linecorp.com.internal.common.micrometer.tracing.TraceContextUtil;

import io.micrometer.tracing.CurrentTraceContext;
import io.micrometer.tracing.TraceContext;

public class RequestContextCurrentTraceContext implements CurrentTraceContext {

    private static final Logger logger = LoggerFactory.getLogger(RequestContextCurrentTraceContext.class);
    private static final ThreadLocal<TraceContext> THREAD_LOCAL_CONTEXT = new ThreadLocal<>();
    private static final ThreadLocal<Boolean> THREAD_NOT_REQUEST_THREAD = new ThreadLocal<>();
    private static final Scope INITIAL_REQUEST_SCOPE = new Scope() {
        @Override
        public void close() {
            // Don't remove the outer-most context (client or server request)
        }

        @Override
        public String toString() {
            return "InitialRequestScope";
        }
    };

    private final List<Pattern> nonRequestThreadPatterns;
    private final boolean scopeDecoratorAdded;

    public RequestContextCurrentTraceContext(List<Pattern> nonRequestThreadPatterns,
                                             boolean scopeDecoratorAdded) {
        this.nonRequestThreadPatterns = nonRequestThreadPatterns;
        this.scopeDecoratorAdded = scopeDecoratorAdded;
    }

    private static void setCurrentThreadNotRequestThread(boolean value) {
        if (value) {
            THREAD_NOT_REQUEST_THREAD.set(true);
        } else {
            THREAD_NOT_REQUEST_THREAD.remove();
        }
    }

    @Override
    @Nullable
    public TraceContext context() {
        final RequestContext ctx = getRequestContextOrWarnOnce();
        if (ctx == null) {
            return THREAD_LOCAL_CONTEXT.get();
        }

        if (!ctx.eventLoop().inEventLoop()) {
            final TraceContext threadLocalContext = THREAD_LOCAL_CONTEXT.get();
            if (threadLocalContext != null) {
                return threadLocalContext;
            }
        }
        return traceContext(ctx);
    }

    @Override
    public Scope newScope(TraceContext context) {
        if (context != null && TraceContextUtil.PingPongExtra.maybeSetPong(context)) {
            return NOOP.newScope(context);
        }

        final RequestContext ctx = getRequestContextOrWarnOnce();

        if (ctx != null && ctx.eventLoop().inEventLoop()) {
            return createScopeForRequestThread(ctx, context);
        } else {
            // The RequestContext is the canonical thread-local storage for the thread processing the request.
            // However, when creating spans on other threads (e.g., a thread-pool), we must use separate
            // thread-local storage to prevent threads from replacing the same trace context.
            return createScopeForNonRequestThread(context);
        }
    }

    @Override
    public Scope maybeScope(TraceContext context) {
        return null;
    }

    @Override
    public <C> Callable<C> wrap(Callable<C> task) {
        return null;
    }

    @Override
    public Runnable wrap(Runnable task) {
        return null;
    }

    @Override
    public Executor wrap(Executor delegate) {
        return null;
    }

    @Override
    public ExecutorService wrap(ExecutorService delegate) {
        return null;
    }

    @Nullable
    private RequestContext getRequestContextOrWarnOnce() {
        if (Boolean.TRUE.equals(THREAD_NOT_REQUEST_THREAD.get())) {
            return null;
        }
        if (!nonRequestThreadPatterns.isEmpty()) {
            final String threadName = Thread.currentThread().getName();
            for (Pattern pattern : nonRequestThreadPatterns) {
                if (pattern.matcher(threadName).find()) {
                    // A matched thread will match forever, so it's worth avoiding this regex match on every
                    // time the thread is used by saving into the ThreadLocal.
                    setCurrentThreadNotRequestThread(true);
                    return null;
                }
            }
        }
        return RequestContext.mapCurrent(Function.identity(), LogRequestContextWarningOnce.INSTANCE);
    }

    private Scope createScopeForRequestThread(RequestContext ctx, @Nullable TraceContext currentSpan) {
        final TraceContext previous = traceContext(ctx);
        setTraceContext(ctx, currentSpan);

        // Don't remove the outer-most context (client or server request)
        if (previous == null) {
            return decorateScope(currentSpan, INITIAL_REQUEST_SCOPE);
        }

        // Removes sub-spans (i.e. local spans) from the current context when Brave's scope does.
        // If an asynchronous sub-span, it may still complete later.
        class RequestContextTraceContextScope implements Scope {
            @Override
            public void close() {
                // re-lookup the attribute to avoid holding a reference to the request if this scope is leaked
                final RequestContext ctx = getRequestContextOrWarnOnce();
                if (ctx != null) {
                    setTraceContext(ctx, previous);
                }
            }

            @Override
            public String toString() {
                return "RequestContextTraceContextScope";
            }
        }

        return decorateScope(currentSpan, new RequestContextTraceContextScope());
    }

    private Scope createScopeForNonRequestThread(@Nullable TraceContext currentSpan) {
        final TraceContext previous = THREAD_LOCAL_CONTEXT.get();
        THREAD_LOCAL_CONTEXT.set(currentSpan);
        class ThreadLocalScope implements Scope {
            @Override
            public void close() {
                THREAD_LOCAL_CONTEXT.set(previous);
            }

            @Override
            public String toString() {
                return "ThreadLocalScope";
            }
        }

        return decorateScope(currentSpan, new ThreadLocalScope());
    }

    private enum LogRequestContextWarningOnce implements Supplier<RequestContext> {

        INSTANCE;

        @Override
        @Nullable
        public RequestContext get() {
            ClassLoaderHack.loadMe();
            return null;
        }

        /**
         * This won't be referenced until {@link #get()} is called. If there's only one classloader, the
         * initializer will only be called once.
         */
        private static final class ClassLoaderHack {
            static {
                logger.warn("Attempted to propagate trace context, but no request context available. " +
                            "Did you forget to use RequestContext.makeContextAware()?",
                            new NoRequestContextException());
            }

            static void loadMe() {}
        }

        private static final class NoRequestContextException extends RuntimeException {
            private static final long serialVersionUID = 2804189311774982052L;
        }
    }
}
