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
package com.linecorp.com.service.micrometer.tracing;

import java.util.function.Function;

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.internal.common.RequestContextExtension;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.SimpleDecoratingHttpService;
import com.linecorp.armeria.server.TransientServiceOption;
import com.linecorp.com.common.micrometer.tracing.RequestContextCurrentTraceContext;

import io.micrometer.tracing.CurrentTraceContext.Scope;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.Tracer.SpanInScope;
import io.micrometer.tracing.http.HttpServerHandler;
import io.micrometer.tracing.http.HttpServerRequest;
import io.micrometer.tracing.http.HttpServerResponse;

public final class TracingService extends SimpleDecoratingHttpService {

    private static final Scope SERVICE_REQUEST_DECORATING_SCOPE = new Scope() {
        @Override
        public void close() {

        }

        @Override
        public String toString() {
            return "ServiceRequestDecoratingScope";
        }
    };

    /**
     * Creates a new tracing {@link HttpService} decorator using the specified {@link Tracer} and
     * {@link HttpServerHandler} instance.
     */
    public static Function<? super HttpService, TracingService> newDecorator(Tracer tracer,
                                                                             HttpServerHandler handler) {
        return service -> new TracingService(service, tracer, handler);
    }

    private final Tracer tracer;
    private final HttpServerHandler handler;
    private final RequestContextCurrentTraceContext currentTraceContext;

    /**
     * Creates a new instance.
     */
    private TracingService(HttpService delegate, Tracer tracer, HttpServerHandler handler) {
        super(delegate);
        this.tracer = tracer;
        this.handler = handler;
        currentTraceContext = (RequestContextCurrentTraceContext) tracer.currentTraceContext();
    }

    @Override
    public HttpResponse serve(ServiceRequestContext ctx, HttpRequest req) throws Exception {
        if (!ctx.config().transientServiceOptions().contains(TransientServiceOption.WITH_TRACING)) {
            return unwrap().serve(ctx, req);
        }

        final HttpServerRequest tracingReq = ServiceRequestContextAdapter.asHtpServerRequest(ctx);
        final Span span = handler.handleReceive(tracingReq);

        final RequestContextExtension ctxExtension = ctx.as(RequestContextExtension.class);
        if (currentTraceContext.scopeDecoratorAdded() && !span.isNoop() && ctxExtension != null) {
            // Run the scope decorators when the ctx is pushed to the thread local.
            ctxExtension.hook(() -> currentTraceContext.decorateScope(span.context(),
                                                                      SERVICE_REQUEST_DECORATING_SCOPE));
        }

        maybeAddTagsToSpan(ctx, tracingReq, span);
        try (SpanInScope ignored = tracer.withSpan(span)) {
            return unwrap().serve(ctx, req);
        }
    }

    private void maybeAddTagsToSpan(ServiceRequestContext ctx, HttpServerRequest tracingReq, Span span) {
        if (span.isNoop()) {
            return;
        }

        ctx.log().whenComplete().thenAccept(log -> {
            span.start(log.requestStartTimeMicros());

            final Long wireReceiveTimeNanos = log.requestFirstBytesTransferredTimeNanos();
            assert wireReceiveTimeNanos != null;
            SpanTags.logWireReceive(span, wireReceiveTimeNanos, log);

            final Long wireSendTimeNanos = log.responseFirstBytesTransferredTimeNanos();
            if (wireSendTimeNanos != null) {
                SpanTags.logWireSend(span, wireSendTimeNanos, log);
            } else {
                // If the client timed-out the request, we will have never sent any response data at all.
            }

            final HttpServerResponse tracingRes =
                    ServiceRequestContextAdapter.asHttpServerResponse(log, tracingReq);
            handler.handleSend(tracingRes, span);
        });
    }
}
