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

import org.apache.inlong.common.pojo.sort.ClusterTagConfig;
import org.apache.inlong.common.pojo.sort.TaskConfig;
import org.apache.inlong.common.pojo.sort.dataflow.DataFlowConfig;
import org.apache.inlong.common.pojo.sort.dataflow.sink.PulsarSinkConfig;
import org.apache.inlong.common.pojo.sort.dataflow.sink.SinkConfig;
import org.apache.inlong.common.pojo.sort.node.PulsarNodeConfig;
import org.apache.inlong.common.pojo.sortstandalone.SortTaskConfig;
import org.apache.inlong.sdk.transform.encode.SinkEncoder;
import org.apache.inlong.sdk.transform.encode.SinkEncoderFactory;
import org.apache.inlong.sdk.transform.pojo.CsvSinkInfo;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.KvSinkInfo;
import org.apache.inlong.sdk.transform.pojo.MapSinkInfo;
import org.apache.inlong.sdk.transform.process.TransformProcessor;
import org.apache.inlong.sort.standalone.channel.ProfileEvent;
import org.apache.inlong.sort.standalone.config.holder.CommonPropertiesHolder;
import org.apache.inlong.sort.standalone.config.holder.SortClusterConfigHolder;
import org.apache.inlong.sort.standalone.config.holder.v2.SortConfigHolder;
import org.apache.inlong.sort.standalone.config.pojo.CacheClusterConfig;
import org.apache.inlong.sort.standalone.config.pojo.InlongId;
import org.apache.inlong.sort.standalone.metrics.SortConfigMetricReporter;
import org.apache.inlong.sort.standalone.metrics.SortMetricItem;
import org.apache.inlong.sort.standalone.metrics.audit.AuditUtils;
import org.apache.inlong.sort.standalone.sink.SinkContext;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@SuppressWarnings("deprecation")
public class PulsarFederationSinkContext extends SinkContext {

    public static final Logger LOG = InlongLoggerFactory.getLogger(PulsarFederationSinkContext.class);
    public static final String KEY_EVENT_HANDLER = "eventHandler";
    private Map<String, PulsarIdConfig> idConfigMap = new ConcurrentHashMap<>();
    private PulsarNodeConfig pulsarNodeConfig;
    private CacheClusterConfig cacheClusterConfig;

    // Map<threadId, Map<dataFlowId,TransformProcess>>
    protected Map<Long, Map<String, TransformProcessor<String, ?>>> transformMap = new ConcurrentHashMap<>();

    public PulsarFederationSinkContext(String sinkName, Context context, Channel channel) {
        super(sinkName, context, channel);
    }

    public void reload() {
        try {
            TaskConfig newTaskConfig = SortConfigHolder.getTaskConfig(taskName);
            SortTaskConfig newSortTaskConfig = SortClusterConfigHolder.getTaskConfig(taskName);
            if (newTaskConfig == null && newSortTaskConfig == null) {
                LOG.error("newSortTaskConfig is null.");
                return;
            }
            if ((newTaskConfig == null || StringUtils.equals(this.taskConfigJson, gson.toJson(newTaskConfig)))
                    && (newSortTaskConfig == null
                            || StringUtils.equals(this.sortTaskConfigJson, gson.toJson(newSortTaskConfig)))) {
                LOG.info("Same sortTaskConfig, do nothing.");
                return;
            }

            if (newTaskConfig != null) {
                PulsarNodeConfig requestNodeConfig = (PulsarNodeConfig) newTaskConfig.getNodeConfig();
                if (pulsarNodeConfig == null || requestNodeConfig.getVersion() > pulsarNodeConfig.getVersion()) {
                    this.pulsarNodeConfig = requestNodeConfig;
                }
            }

            this.replaceConfig(newTaskConfig, newSortTaskConfig);

            CacheClusterConfig clusterConfig = new CacheClusterConfig();
            clusterConfig.setClusterName(this.taskName);
            if (this.sortTaskConfig != null) {
                clusterConfig.setParams(this.sortTaskConfig.getSinkParams());
            }
            this.cacheClusterConfig = clusterConfig;

            Map<String, PulsarIdConfig> fromTaskConfig = fromTaskConfig(taskConfig);
            Map<String, PulsarIdConfig> fromSortTaskConfig = fromSortTaskConfig(sortTaskConfig);
            SortConfigMetricReporter.reportClusterDiff(clusterId, taskName, fromTaskConfig, fromSortTaskConfig);
            if (unifiedConfiguration) {
                this.idConfigMap = fromTaskConfig;
                this.transformMap.clear();
            } else {
                this.idConfigMap = fromSortTaskConfig;
            }
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public Map<String, PulsarIdConfig> fromTaskConfig(TaskConfig taskConfig) {
        if (taskConfig == null) {
            return new HashMap<>();
        }
        return taskConfig.getClusterTagConfigs()
                .stream()
                .map(ClusterTagConfig::getDataFlowConfigs)
                .flatMap(Collection::stream)
                .map(PulsarIdConfig::create)
                .collect(Collectors.toMap(
                        config -> InlongId.generateUid(config.getInlongGroupId(), config.getInlongStreamId()),
                        v -> v,
                        (v1, v2) -> v1));
    }

    public Map<String, PulsarIdConfig> fromSortTaskConfig(SortTaskConfig sortTaskConfig) {
        if (sortTaskConfig == null) {
            return new HashMap<>();
        }
        Map<String, PulsarIdConfig> newIdConfigMap = new ConcurrentHashMap<>();
        List<Map<String, String>> idList = sortTaskConfig.getIdParams();
        for (Map<String, String> idParam : idList) {
            try {
                PulsarIdConfig idConfig = new PulsarIdConfig(idParam);
                newIdConfigMap.put(idConfig.getUid(), idConfig);
            } catch (Exception e) {
                LOG.error("fail to parse pulsar id config", e);
            }
        }
        return newIdConfigMap;
    }

    public String getTopic(String uid) {
        PulsarIdConfig idConfig = this.idConfigMap.get(uid);
        if (idConfig == null) {
            throw new NullPointerException("uid " + uid + " got null id config");
        }
        return idConfig.getTopic();
    }

    public PulsarIdConfig getIdConfig(String uid) {
        PulsarIdConfig idConfig = this.idConfigMap.get(uid);
        if (idConfig == null) {
            throw new NullPointerException("uid " + uid + "got null PulsarIdConfig");
        }
        return idConfig;
    }

    public PulsarNodeConfig getNodeConfig() {
        return pulsarNodeConfig;
    }

    public CacheClusterConfig getCacheClusterConfig() {
        return cacheClusterConfig;
    }

    public void addSendMetric(ProfileEvent currentRecord, String topic) {
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put(SortMetricItem.KEY_CLUSTER_ID, this.getClusterId());
        dimensions.put(SortMetricItem.KEY_TASK_NAME, this.getTaskName());
        // metric
        fillInlongId(currentRecord, dimensions);
        dimensions.put(SortMetricItem.KEY_SINK_ID, this.getSinkName());
        dimensions.put(SortMetricItem.KEY_SINK_DATA_ID, topic);
        long msgTime = currentRecord.getRawLogTime();
        long auditFormatTime = msgTime - msgTime % CommonPropertiesHolder.getAuditFormatInterval();
        dimensions.put(SortMetricItem.KEY_MESSAGE_TIME, String.valueOf(auditFormatTime));
        SortMetricItem metricItem = this.getMetricItemSet().findMetricItem(dimensions);
        long count = 1;
        long size = currentRecord.getBody().length;
        metricItem.sendCount.addAndGet(count);
        metricItem.sendSize.addAndGet(size);
    }

    /**
     * addReadFailMetric
     */
    public void addSendFailMetric() {
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put(SortMetricItem.KEY_CLUSTER_ID, this.getClusterId());
        dimensions.put(SortMetricItem.KEY_SINK_ID, this.getSinkName());
        long msgTime = System.currentTimeMillis();
        long auditFormatTime = msgTime - msgTime % CommonPropertiesHolder.getAuditFormatInterval();
        dimensions.put(SortMetricItem.KEY_MESSAGE_TIME, String.valueOf(auditFormatTime));
        SortMetricItem metricItem = this.getMetricItemSet().findMetricItem(dimensions);
        metricItem.readFailCount.incrementAndGet();
    }

    /**
     * addSendResultMetric
     *
     * @param currentRecord
     * @param topic
     * @param result
     * @param sendTime
     */
    public void addSendResultMetric(ProfileEvent currentRecord, String topic, boolean result, long sendTime) {
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put(SortMetricItem.KEY_CLUSTER_ID, this.getClusterId());
        dimensions.put(SortMetricItem.KEY_TASK_NAME, this.getTaskName());
        // metric
        fillInlongId(currentRecord, dimensions);
        dimensions.put(SortMetricItem.KEY_SINK_ID, this.getSinkName());
        dimensions.put(SortMetricItem.KEY_SINK_DATA_ID, topic);
        long msgTime = currentRecord.getRawLogTime();
        long auditFormatTime = msgTime - msgTime % CommonPropertiesHolder.getAuditFormatInterval();
        dimensions.put(SortMetricItem.KEY_MESSAGE_TIME, String.valueOf(auditFormatTime));
        SortMetricItem metricItem = this.getMetricItemSet().findMetricItem(dimensions);
        long count = 1;
        long size = currentRecord.getBody().length;
        if (result) {
            metricItem.sendSuccessCount.addAndGet(count);
            metricItem.sendSuccessSize.addAndGet(size);
            AuditUtils.add(AuditUtils.AUDIT_ID_SEND_SUCCESS, currentRecord);
            if (sendTime > 0) {
                long currentTime = System.currentTimeMillis();
                long sinkDuration = currentTime - sendTime;
                long nodeDuration = currentTime - currentRecord.getFetchTime();
                long wholeDuration = currentTime - currentRecord.getRawLogTime();
                metricItem.sinkDuration.addAndGet(sinkDuration * count);
                metricItem.nodeDuration.addAndGet(nodeDuration * count);
                metricItem.wholeDuration.addAndGet(wholeDuration * count);
            }
        } else {
            metricItem.sendFailCount.addAndGet(count);
            metricItem.sendFailSize.addAndGet(size);
        }
    }

    /**
     * create IEvent2PulsarRecordHandler
     *
     * @return IEvent2PulsarRecordHandler
     */
    public IEvent2PulsarRecordHandler createEventHandler() {
        // IEvent2ProducerRecordHandler
        String strHandlerClass = CommonPropertiesHolder.getString(KEY_EVENT_HANDLER,
                DefaultEvent2PulsarRecordHandler.class.getName());
        try {
            Class<?> handlerClass = ClassUtils.getClass(strHandlerClass);
            Object handlerObject = handlerClass.getDeclaredConstructor().newInstance();
            if (handlerObject instanceof IEvent2PulsarRecordHandler) {
                IEvent2PulsarRecordHandler handler = (IEvent2PulsarRecordHandler) handlerObject;
                return handler;
            }
        } catch (Throwable t) {
            LOG.error("Fail to init IEvent2PulsarRecordHandler,handlerClass:{},error:{}",
                    strHandlerClass, t.getMessage(), t);
        }
        return null;
    }

    public TransformProcessor<String, ?> getTransformProcessor(String dataFlowId) {
        Long threadId = Thread.currentThread().getId();
        Map<String, TransformProcessor<String, ?>> transformProcessors = this.transformMap.get(threadId);
        if (transformProcessors == null) {
            transformProcessors = reloadTransform(taskConfig);
            this.transformMap.put(threadId, transformProcessors);
        }
        return transformProcessors.get(dataFlowId);
    }

    private Map<String, TransformProcessor<String, ?>> reloadTransform(TaskConfig taskConfig) {
        ImmutableMap.Builder<String, TransformProcessor<String, ?>> builder = new ImmutableMap.Builder<>();

        taskConfig.getClusterTagConfigs()
                .stream()
                .map(ClusterTagConfig::getDataFlowConfigs)
                .flatMap(Collection::stream)
                .forEach(flow -> {
                    if (StringUtils.isEmpty(flow.getTransformSql())) {
                        return;
                    }
                    TransformProcessor<String, ?> transformProcessor = createTransform(flow);
                    if (transformProcessor == null) {
                        return;
                    }
                    builder.put(flow.getDataflowId(),
                            transformProcessor);
                });

        return builder.build();
    }

    private TransformProcessor<String, ?> createTransform(DataFlowConfig dataFlowConfig) {
        try {
            LOG.info("try to create transform:{}", dataFlowConfig.toString());
            return TransformProcessor.create(
                    createTransformConfig(dataFlowConfig),
                    createSourceDecoder(dataFlowConfig.getSourceConfig()),
                    createSinkEncoder(dataFlowConfig.getSinkConfig()));
        } catch (Exception e) {
            LOG.error("failed to reload transform of dataflow={}, ex={}", dataFlowConfig.getDataflowId(),
                    e.getMessage(), e);
            return null;
        }
    }

    private SinkEncoder<?> createSinkEncoder(SinkConfig sinkConfig) {
        if (!(sinkConfig instanceof PulsarSinkConfig)) {
            throw new IllegalArgumentException("sinkInfo must be an instance of PulsarSinkConfig");
        }
        PulsarSinkConfig sSinkConfig = (PulsarSinkConfig) sinkConfig;
        List<FieldInfo> fieldInfos = sSinkConfig.getFieldConfigs()
                .stream()
                .map(config -> new FieldInfo(config.getName(), deriveTypeConverter(config.getFormatInfo())))
                .collect(Collectors.toList());

        if (StringUtils.equalsIgnoreCase(PulsarSinkConfig.MESSAGE_TYPE_CSV, sSinkConfig.getMessageType())) {
            Character delimiter = sSinkConfig.getDelimiter();
            if (delimiter == null) {
                delimiter = PulsarSinkConfig.CSV_DEFAULT_DELIMITER;
            }
            CsvSinkInfo sinkInfo = new CsvSinkInfo(sinkConfig.getEncodingType(), delimiter, sSinkConfig.getEscapeChar(),
                    fieldInfos);
            return SinkEncoderFactory.createCsvEncoder(sinkInfo);
        } else if (StringUtils.equalsIgnoreCase(PulsarSinkConfig.MESSAGE_TYPE_KV, sSinkConfig.getMessageType())) {
            Character entrySplitter = sSinkConfig.getEntrySplitter();
            if (entrySplitter == null) {
                entrySplitter = PulsarSinkConfig.KV_DEFAULT_ENTRYSPLITTER;
            }
            Character kvSplitter = sSinkConfig.getKvSplitter();
            if (kvSplitter == null) {
                kvSplitter = PulsarSinkConfig.KV_DEFAULT_KVSPLITTER;
            }
            KvSinkInfo sinkInfo = new KvSinkInfo(sinkConfig.getEncodingType(), fieldInfos);
            sinkInfo.setEntryDelimiter(entrySplitter);
            sinkInfo.setKvDelimiter(kvSplitter);
            return SinkEncoderFactory.createKvEncoder(sinkInfo);
        } else if (StringUtils.equalsIgnoreCase(PulsarSinkConfig.MESSAGE_TYPE_JSON, sSinkConfig.getMessageType())) {
            MapSinkInfo sinkInfo = new MapSinkInfo(sinkConfig.getEncodingType(), fieldInfos);
            return SinkEncoderFactory.createMapEncoder(sinkInfo);
        } else {
            CsvSinkInfo sinkInfo = new CsvSinkInfo(sinkConfig.getEncodingType(), PulsarSinkConfig.CSV_DEFAULT_DELIMITER,
                    null, fieldInfos);
            return SinkEncoderFactory.createCsvEncoder(sinkInfo);
        }
    }
}
