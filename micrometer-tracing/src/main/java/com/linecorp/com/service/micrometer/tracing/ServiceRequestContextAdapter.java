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

import java.util.Collection;
import java.util.stream.Collectors;

import com.linecorp.armeria.common.annotation.Nullable;
import com.linecorp.armeria.common.logging.RequestLog;
import com.linecorp.armeria.server.ServiceRequestContext;

import io.micrometer.tracing.http.HttpServerRequest;
import io.micrometer.tracing.http.HttpServerResponse;

final class ServiceRequestContextAdapter {

    private ServiceRequestContextAdapter() {}

    static HttpServerRequest asHtpServerRequest(ServiceRequestContext ctx) {
        return new HttpServerRequestImpl(ctx);
    }

    static HttpServerResponse asHttpServerResponse(RequestLog log, HttpServerRequest request) {
        return new HttpServerResponseImpl(log, request);
    }

    private static final class HttpServerRequestImpl implements HttpServerRequest {
        private final ServiceRequestContext ctx;

        HttpServerRequestImpl(ServiceRequestContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public ServiceRequestContext unwrap() {
            return ctx;
        }

        @Override
        public String method() {
            return ctx.method().name();
        }

        @Override
        public String path() {
            return ctx.path();
        }

        @Override
        public String route() {
            return ctx.config().route().patternString();
        }

        @Override
        @Nullable
        public String url() {
            return ctx.request().uri().toString();
        }

        @Override
        @Nullable
        public String header(String name) {
            return ctx.request().headers().get(name);
        }

        @Override
        public Collection<String> headerNames() {
            return ctx.request().headers().stream()
                      .map(e -> e.getKey().toString())
                      .collect(Collectors.toUnmodifiableList());
        }
    }

    private static final class HttpServerResponseImpl implements HttpServerResponse {
        private final RequestLog log;
        private final HttpServerRequest request;

        HttpServerResponseImpl(RequestLog log, HttpServerRequest request) {
            assert log.isComplete() : log;
            this.log = log;
            this.request = request;
        }

        @Override
        public ServiceRequestContext unwrap() {
            return (ServiceRequestContext) log.context();
        }

        @Override
        @Nullable
        public Throwable error() {
            return log.responseCause();
        }

        @Override
        public int statusCode() {
            return log.responseHeaders().status().code();
        }

        @Override
        public HttpServerRequest request() {
            return request;
        }

        @Override
        @Nullable
        public String header(String header) {
            return log.responseHeaders().get(header);
        }

        @Override
        public Collection<String> headerNames() {
            return log.responseHeaders().stream()
                      .map(e -> e.getKey().toString())
                      .collect(Collectors.toUnmodifiableList());
        }
    }
}
