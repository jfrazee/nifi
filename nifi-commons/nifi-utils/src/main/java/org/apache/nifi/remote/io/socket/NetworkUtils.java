/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
*/
package org.apache.nifi.remote.io.socket;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkUtils {

    private static final Logger logger = LoggerFactory.getLogger(NetworkUtils.class); 

    /**
     * Will determine the available port
     */
    public final static int availablePort() {
        return availablePort(5);
    }

    public final static int availablePort(final int numTries) {
        return availablePort(numTries, "localhost");
    }

    public final static int availablePort(final int numTries, final String hostname) {
        for (int i = numTries; i > 0; i--) {
            try (final ServerSocket s = new ServerSocket(0, 1, InetAddress.getByName(hostname))) {
                s.setReuseAddress(true);
                if (s.isBound()) {
                    int port = s.getLocalPort();
                    if (logger.isDebugEnabled()) { 
                        logger.debug("Found port available on {}", port); 
                    }
                    return s.getLocalPort();
                }
            } catch (final Exception ignore) {} finally {
                try { Thread.sleep(10); } catch (final Exception ignore) {}
            }
        }

        throw new IllegalStateException("Failed to discover available port.");
    }

}
