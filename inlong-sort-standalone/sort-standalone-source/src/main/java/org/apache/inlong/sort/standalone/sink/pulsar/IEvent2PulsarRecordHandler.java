/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.standalone.sink.pulsar;

import org.apache.inlong.sort.standalone.channel.ProfileEvent;

import java.io.IOException;
import java.util.List;

/**
 * 
 * IEvent2PulsarRecordHandler
 */
public interface IEvent2PulsarRecordHandler {

    /**
     * parse the event into one or more pulsar message payloads.
     *
     * @param  context     pulsar federation sink context
     * @param  event       raw profile event
     * @param  idConfig    id config resolved from event uid
     * @return             a list of message payload byte arrays; empty/null means filtered
     * @throws IOException on any IO error
     */
    List<byte[]> parse(PulsarFederationSinkContext context, ProfileEvent event, PulsarIdConfig idConfig)
            throws IOException;
}
