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
package org.apache.rocketmq.broker.longpolling;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.DefaultMessageFilter;
import org.apache.rocketmq.store.MessageFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 拉取消息请求挂起维护线程服务
 */
public class PullRequestHoldService extends ServiceThread {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private static final String TOPIC_QUEUEID_SEPARATOR = "@";

    private final BrokerController brokerController;

    private final SystemClock systemClock = new SystemClock();
    /**
     * 消息过滤器
     */
    private final MessageFilter messageFilter = new DefaultMessageFilter();
    /**
     * 拉取消息请求集合
     */
    private ConcurrentHashMap<String/* topic@queueId */, ManyPullRequest> pullRequestTable =
            new ConcurrentHashMap<>(1024);

    public PullRequestHoldService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 添加拉取消息挂起请求
     *
     * @param topic 主题
     * @param queueId 队列编号
     * @param pullRequest 拉取消息请求
     */
    public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
        String key = this.buildKey(topic, queueId);
        // 添加到本地信息表中
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }

        mpr.addPullRequest(pullRequest);
    }

    /**
     * 根据 主题 + 队列编号 创建唯一标识
     *
     * @param topic 主题
     * @param queueId 队列编号
     * @return key
     */
    private String buildKey(final String topic, final int queueId) {
        StringBuilder sb = new StringBuilder();
        sb.append(topic);
        sb.append(TOPIC_QUEUEID_SEPARATOR);
        sb.append(queueId);
        return sb.toString();
    }

    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        // 判断拉取消息请求挂起维护线程服务是否开启
        while (!this.isStopped()) {
            try {
                // 根据 长轮训 还是 短轮训 设置不同的等待时间
                if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                    // 长轮训等五秒开始检查请求是否需要通知
                    this.waitForRunning(5 * 1000);
                } else {
                    // 短轮训等待时间，默认一秒，开始检查请求是否需要通知
                    this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMills());
                }
                // 检查挂起请求是否有需要通知的
                long beginLockTimestamp = this.systemClock.now();
                this.checkHoldRequest();
                // 打印花费时间日志
                long costTime = this.systemClock.now() - beginLockTimestamp;
                if (costTime > 5 * 1000) {
                    log.info("[NOTIFYME] check hold request cost {} ms.", costTime);
                }
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info("{} service end", this.getServiceName());
    }

    @Override
    public String getServiceName() {
        return PullRequestHoldService.class.getSimpleName();
    }

    /**
     * 遍历挂起请求，检查是否有需要通知的请求。
     */
    private void checkHoldRequest() {
        // key: topic@queueId
        for (String key : this.pullRequestTable.keySet()) {
            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            // 判断格式是否正确
            if (2 == kArray.length) {
                String topic = kArray[0];
                int queueId = Integer.parseInt(kArray[1]);
                // 获取目标消费队列的当前最新消息位置
                final long offset = this.brokerController.getMessageStore().getMaxOffsetInQuque(topic, queueId);
                try {
                    this.notifyMessageArriving(topic, queueId, offset);
                } catch (Throwable e) {
                    log.error("check hold request failed. topic={}, queueId={}", topic, queueId, e);
                }
            }
        }
    }

    /**
     * 检查是否有需要通知的请求
     *
     * @param topic 主题
     * @param queueId 队列编号
     * @param maxOffset 消费队列最大offset
     */
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset) {
        notifyMessageArriving(topic, queueId, maxOffset, null);
    }

    /**
     * 检查是否有需要通知的请求
     *
     * @param topic 主题
     * @param queueId 队列编号
     * @param maxOffset 消费队列最大offset
     * @param tagsCode 过滤tagsCode
     */
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset, final Long tagsCode) {
        String key = this.buildKey(topic, queueId);
        // 重新拼key，获取拉取请求
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (mpr != null) {
            // 拷贝一份这个topic、queueId下的所有拉取请求
            List<PullRequest> requestList = mpr.cloneListAndClear();
            if (requestList != null) {
                List<PullRequest> replayList = new ArrayList<>(); // 不符合唤醒的请求数组
                // 遍历这些请求
                for (PullRequest request : requestList) {
                    // 如果 maxOffset 过小，则重新读取一次。
                    long newestOffset = maxOffset; // 最新的偏移量
                    if (newestOffset <= request.getPullFromThisOffset()) {
                        // 要消费的消息进度在当前进度的后面，更新偏移量为当前最新的偏移量
                        newestOffset = this.brokerController.getMessageStore().getMaxOffsetInQuque(topic, queueId);
                    }
                    // 【进度满足以及超过request中设定的挂起时间（当前时间 >= 暂停时间点 + 超时时间）同样会唤醒请求，也就是再次拉取信息】
                    // 有新的匹配消息，唤醒请求，即再次拉取消息。
                    if (newestOffset > request.getPullFromThisOffset()) {
                        // 有消息可以读取，进度满足
                        // 判断是否满足匹配关系
                        if (this.messageFilter.isMessageMatched(request.getSubscriptionData(), tagsCode)) {
                            try {
                                // 底层使用线程池，不会阻塞，异步执行
                                this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                    request.getRequestCommand());
                            } catch (Throwable e) {
                                log.error("execute request when wakeup failed.", e);
                            }
                            continue;
                        }
                    }
                    // 超过挂起时间，唤醒请求，即再次拉取消息。
                    if (System.currentTimeMillis() >= (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                        try {
                            // 底层使用线程池，不会阻塞，异步执行
                            this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                request.getRequestCommand());
                        } catch (Throwable e) {
                            log.error("execute request when wakeup failed.", e);
                        }
                        continue;
                    }
                    // 不符合再次拉取的请求，再次添加回去
                    replayList.add(request);
                }
                // 添加回去
                if (!replayList.isEmpty()) {
                    mpr.addPullRequest(replayList);
                }
            }
        }
    }
}
