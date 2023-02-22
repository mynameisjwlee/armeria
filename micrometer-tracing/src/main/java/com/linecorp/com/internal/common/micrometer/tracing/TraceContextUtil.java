/*
 * Copyright 2019 LINE Corporation
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

package com.linecorp.com.internal.common.micrometer.tracing;

import com.linecorp.armeria.common.RequestContext;
import com.linecorp.armeria.common.annotation.Nullable;

import io.micrometer.tracing.TraceContext;
import io.netty.util.AttributeKey;

public final class TraceContextUtil {

    private static final AttributeKey<TraceContext> TRACE_CONTEXT_KEY =
            AttributeKey.valueOf(TraceContextUtil.class, "TRACE_CONTEXT");

    @Nullable
    public static TraceContext traceContext(RequestContext ctx) {
        return ctx.attr(TRACE_CONTEXT_KEY);
    }

    public static void setTraceContext(RequestContext ctx, TraceContext traceContext) {
        ctx.setAttr(TRACE_CONTEXT_KEY, traceContext);
    }

    private TraceContextUtil() {}

    /** Hack to allow us to peek inside a current trace context implementation. */
    public static final class PingPongExtra {
        /**
         * If the input includes only this extra, set {@link #isPong() pong = true}.
         */
        public static boolean maybeSetPong(TraceContext context) {
            if (context.extra().size() == 1) {
                final Object extra = context.extra().get(0);
                if (extra instanceof PingPongExtra) {
                    ((PingPongExtra) extra).pong = true;
                    return true;
                }
            }
            return false;
        }

        private boolean pong;

        public boolean isPong() {
            return pong;
        }
    }
}
