/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.facebook;

import com.restfb.exception.FacebookGraphException;

public class FacebookRateLimitException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private Integer errorCode;

    private Long resetTime;

    public FacebookRateLimitException(String message, Integer errorCode, Long resetTime) {
        super(message);
        this.errorCode = errorCode;
        this.resetTime = resetTime;
    }

    public FacebookRateLimitException(String message, Throwable cause, Integer errorCode, Long resetTime) {
        super(message, cause);
        this.errorCode = errorCode;
        this.resetTime = resetTime;
    }

    public FacebookRateLimitException(FacebookGraphException e, Long resetTime) {
        super(e);
        this.errorCode = e.getErrorCode();
        this.resetTime = resetTime;
    }

    public Integer getErrorCode() {
        return errorCode;
    }

    public Long getResetTime() {
        return resetTime;
    }

}
