/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.playtika.shepherd.inernal;

import java.util.Arrays;
import java.util.Locale;

public enum Protocol {
    SIMPLE {
        @Override
        public String protocol() {
            return "simple-lz4";
        }
    };

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    /**
     * Return the name of the protocol that this mode will use in {@code ProtocolMetadata}.
     *
     * @return the protocol name
     */
    public abstract String protocol();

    /**
     * Return the enum that corresponds to the protocol name that is given as an argument;
     * if no mapping is found {@code IllegalArgumentException} is thrown.
     */
    public static Protocol fromProtocol(String protocolName) {
        return Arrays.stream(Protocol.values())
                .filter(mode -> mode.protocol().equalsIgnoreCase(protocolName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Not found protocol for protocol name: " + protocolName));
    }
}
