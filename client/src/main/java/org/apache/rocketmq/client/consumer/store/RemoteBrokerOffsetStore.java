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
package org.apache.rocketmq.client.consumer.store;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Remote storage implementation
 */
public class RemoteBrokerOffsetStore implements OffsetStore {
    private final static Logger log = ClientLogger.getLog();
    private final MQClientInstance mQClientFactory;
    /**
     * 消费分组
     */
    private final String groupName;
    /**
     * 消费进度
     */
    private ConcurrentHashMap<MessageQueue, AtomicLong> offsetTable = new ConcurrentHashMap<>();

    public RemoteBrokerOffsetStore(MQClientInstance mQClientFactory, String groupName) {
        this.mQClientFactory = mQClientFactory;
        this.groupName = groupName;
    }

    @Override
    public void load() {
        // 不进行加载，实际读取消费进度时，从 Broker 获取。
    }

    @Override
    public void updateOffset(MessageQueue mq, long offset, boolean increaseOnly) {
        if (mq != null) {
            AtomicLong offsetOld = this.offsetTable.get(mq);
            if (null == offsetOld) {
                offsetOld = this.offsetTable.putIfAbsent(mq, new AtomicLong(offset));
            }

            if (null != offsetOld) {
                if (increaseOnly) {
                    MixAll.compareAndIncreaseOnly(offsetOld, offset);
                } else {
                    offsetOld.set(offset);
                }
            }
        }
    }

    @Override
    public long readOffset(final MessageQueue mq, final ReadOffsetType type) {
        if (mq != null) {
            switch (type) {
                // 优先从内存读取，读取不到，从存储读取。
                case MEMORY_FIRST_THEN_STORE:
                // 从内存读取
                case READ_FROM_MEMORY: {
                    AtomicLong offset = this.offsetTable.get(mq);
                    if (offset != null) {
                        return offset.get();
                    } else if (ReadOffsetType.READ_FROM_MEMORY == type) {
                        return -1;
                    }
                }
                // 从存储( Broker 或 文件 )读取
                case READ_FROM_STORE: {
                    try {
                        long brokerOffset = this.fetchConsumeOffsetFromBroker(mq);
                        AtomicLong offset = new AtomicLong(brokerOffset);
                        // 更新本地进度缓存
                        this.updateOffset(mq, offset.get(), false);
                        return brokerOffset;
                    }
                    // No offset in broker
                    catch (MQBrokerException e) {
                        return -1;
                    }
                    //Other exceptions
                    catch (Exception e) {
                        log.warn("fetchConsumeOffsetFromBroker exception, " + mq, e);
                        return -2;
                    }
                }
                default:
                    break;
            }
        }

        return -1;
    }

    /**
     * 持久化指定消息队列数组的消费进度到Broker，并移除非指定消息队列
     *
     * @param mqs 指定消息队列
     */
    @Override
    public void persistAll(Set<MessageQueue> mqs) {
        if (null == mqs || mqs.isEmpty())
            return;

        // 持久化消息队列
        final HashSet<MessageQueue> unusedMQ = new HashSet<>();
        if (!mqs.isEmpty()) {
            for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
                MessageQueue mq = entry.getKey();
                AtomicLong offset = entry.getValue();
                if (offset != null) {
                    if (mqs.contains(mq)) {
                        try {
                            // 更新broker中的进度
                            this.updateConsumeOffsetToBroker(mq, offset.get());
                            log.info("[persistAll] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}",
                                this.groupName,
                                this.mQClientFactory.getClientId(),
                                mq,
                                offset.get());
                        } catch (Exception e) {
                            log.error("updateConsumeOffsetToBroker exception, " + mq.toString(), e);
                        }
                    } else {
                        unusedMQ.add(mq);
                    }
                }
            }
        }

        // 本地移除不适用的消息队列
        if (!unusedMQ.isEmpty()) {
            for (MessageQueue mq : unusedMQ) {
                this.offsetTable.remove(mq);
                log.info("remove unused mq, {}, {}", mq, this.groupName);
            }
        }
    }

    /**
     * 持久化队列消费进度到Broker
     *
     * @param mq MQ
     */
    @Override
    public void persist(MessageQueue mq) {
        AtomicLong offset = this.offsetTable.get(mq);
        if (offset != null) {
            try {
                this.updateConsumeOffsetToBroker(mq, offset.get());
                log.info("[persist] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}",
                    this.groupName,
                    this.mQClientFactory.getClientId(),
                    mq,
                    offset.get());
            } catch (Exception e) {
                log.error("updateConsumeOffsetToBroker exception, " + mq.toString(), e);
            }
        }
    }

    public void removeOffset(MessageQueue mq) {
        if (mq != null) {
            this.offsetTable.remove(mq);
            log.info("remove unnecessary messageQueue offset. group={}, mq={}, offsetTableSize={}", this.groupName, mq,
                offsetTable.size());
        }
    }

    @Override
    public Map<MessageQueue, Long> cloneOffsetTable(String topic) {
        Map<MessageQueue, Long> cloneOffsetTable = new HashMap<MessageQueue, Long>();
        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            if (!UtilAll.isBlank(topic) && !topic.equals(mq.getTopic())) {
                continue;
            }
            cloneOffsetTable.put(mq, entry.getValue().get());
        }
        return cloneOffsetTable;
    }

    /**
     * Update the Consumer Offset in one way, once the Master is off, updated to Slave,
     * here need to be optimized.
     */
    private void updateConsumeOffsetToBroker(MessageQueue mq, long offset) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException {
        updateConsumeOffsetToBroker(mq, offset, true);
    }

    /**
     * Update the Consumer Offset synchronously, once the Master is off, updated to Slave,
     * here need to be optimized.
     */
    @Override
    public void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        if (null == findBrokerResult) {
            // TODO Here may be heavily overhead for Name Server,need tuning
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        }

        if (findBrokerResult != null) {
            UpdateConsumerOffsetRequestHeader requestHeader = new UpdateConsumerOffsetRequestHeader();
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setConsumerGroup(this.groupName);
            requestHeader.setQueueId(mq.getQueueId());
            requestHeader.setCommitOffset(offset);

            if (isOneway) {
                this.mQClientFactory.getMQClientAPIImpl().updateConsumerOffsetOneway(
                    findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
            } else {
                this.mQClientFactory.getMQClientAPIImpl().updateConsumerOffset(
                    findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
            }
        } else {
            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }
    }

    private long fetchConsumeOffsetFromBroker(MessageQueue mq) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        if (null == findBrokerResult) {
            // TODO Here may be heavily overhead for Name Server,need tuning
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        }

        if (findBrokerResult != null) {
            QueryConsumerOffsetRequestHeader requestHeader = new QueryConsumerOffsetRequestHeader();
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setConsumerGroup(this.groupName);
            requestHeader.setQueueId(mq.getQueueId());

            return this.mQClientFactory.getMQClientAPIImpl().queryConsumerOffset(
                findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
        } else {
            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }
    }
}
