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

import org.apache.inlong.sdk.commons.protocol.EventConstants;
import org.apache.inlong.sdk.transform.process.TransformProcessor;
import org.apache.inlong.sort.standalone.channel.ProfileEvent;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;

import com.google.gson.Gson;
import org.slf4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 
 * DefaultEvent2PulsarRecordHandler
 */
public class DefaultEvent2PulsarRecordHandler implements IEvent2PulsarRecordHandler {

    public static final Logger LOG = InlongLoggerFactory.getLogger(DefaultEvent2PulsarRecordHandler.class);

    public static final String KEY_EXTINFO = "extinfo";
    protected final ByteArrayOutputStream outMsg = new ByteArrayOutputStream();
    protected final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    protected final Date currentDate = new Date();
    protected final Gson gson = new Gson();

    /**
     * parse
     *
     * @param  context     pulsar federation sink context
     * @param  event       raw profile event
     * @param  idConfig    id config resolved from event uid
     * @return             a list of message payload byte arrays
     * @throws IOException on any IO error
     */
    @Override
    public List<byte[]> parse(PulsarFederationSinkContext context, ProfileEvent event, PulsarIdConfig idConfig)
            throws IOException {
        TransformProcessor<String, ?> processor = context.getTransformProcessor(idConfig.getDataFlowId());
        if (processor != null) {
            return this.parseByTransform(context, event, processor);
        } else {
            byte[] record = this.parseByBytes(event, idConfig);
            return Arrays.asList(record);
        }
    }

    public List<byte[]> parseByTransform(PulsarFederationSinkContext context, ProfileEvent event,
            TransformProcessor<String, ?> processor) throws IOException {
        // extParams
        Map<String, Object> extParams = new ConcurrentHashMap<>();
        extParams.putAll(context.getSinkContext().getParameters());
        event.getHeaders().forEach((k, v) -> extParams.put(k, v));
        // transform
        List<?> results = processor.transformForBytes(event.getBody(), extParams);
        if (results == null) {
            return new ArrayList<>();
        }
        // build
        List<byte[]> records = new ArrayList<>(results.size());
        for (Object result : results) {
            byte[] msgContent;
            if (result instanceof String) {
                msgContent = result.toString().getBytes();
            } else if (result instanceof byte[]) {
                msgContent = (byte[]) result;
            } else {
                msgContent = gson.toJson(result).getBytes();
            }
            records.add(msgContent);
        }
        return records;
    }

    public byte[] parseByBytes(ProfileEvent event, PulsarIdConfig idConfig) throws IOException {
        String delimiter = idConfig.getSeparator();
        byte separator = (byte) delimiter.charAt(0);
        outMsg.reset();
        switch (idConfig.getDataType()) {
            case TEXT:
                currentDate.setTime(event.getRawLogTime());
                String ftime = dateFormat.format(currentDate);
                outMsg.write(ftime.getBytes());
                outMsg.write(separator);
                String extinfo = getExtInfo(event);
                outMsg.write(extinfo.getBytes());
                outMsg.write(separator);
                break;
            case PB:
            case JCE:
            case UNKNOWN:
                break;
            default:
                break;
        }
        outMsg.write(event.getBody());
        return outMsg.toByteArray();
    }

    /**
     * getExtInfo
     * 
     * @param  event
     * @return
     */
    public String getExtInfo(ProfileEvent event) {
        String extinfoValue = event.getHeaders().get(KEY_EXTINFO);
        if (extinfoValue != null) {
            return KEY_EXTINFO + "=" + extinfoValue;
        }
        extinfoValue = KEY_EXTINFO + "=" + event.getHeaders().get(EventConstants.HEADER_KEY_SOURCE_IP);
        return extinfoValue;
    }
}
