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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.common.ThreadLocalIndex;

import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 延迟故障容错实现。维护每个对象的信息。
 */
public class LatencyFaultToleranceImpl implements LatencyFaultTolerance<String> {

    /**
     * 对象故障信息Table
     * key -> broker Name
     * value -> 这个broker的故障信息对象
     */
    private final ConcurrentHashMap<String, FaultItem> faultItemTable = new ConcurrentHashMap<>(16);
    /**
     * 对象选择Index
     * @see #pickOneAtLeast()
     */
    private final ThreadLocalIndex whichItemWorst = new ThreadLocalIndex();

    @Override
    public void updateFaultItem(final String name, final long currentLatency, final long notAvailableDuration) {
        // 根据名称获取到老的故障信息
        FaultItem old = this.faultItemTable.get(name);
        if (null == old) {
            // 不存在于故障表中
            // 创建对象
            final FaultItem faultItem = new FaultItem(name);
            faultItem.setCurrentLatency(currentLatency);
            faultItem.setStartTimestamp(System.currentTimeMillis() + notAvailableDuration);
            // 更新对象
            old = this.faultItemTable.putIfAbsent(name, faultItem);
            if (old != null) {
                old.setCurrentLatency(currentLatency);
                old.setStartTimestamp(System.currentTimeMillis() + notAvailableDuration);
            }
        } else { // 更新对象
            // 如果故障表中存在了这个broker的信息那就更新它的可用时间
            old.setCurrentLatency(currentLatency);
            old.setStartTimestamp(System.currentTimeMillis() + notAvailableDuration);
        }
    }

    @Override
    public boolean isAvailable(final String name) {
        final FaultItem faultItem = this.faultItemTable.get(name);
        if (faultItem != null) {
            return faultItem.isAvailable();
        }
        return true;
    }

    @Override
    public void remove(final String name) {
        this.faultItemTable.remove(name);
    }

    /**
     * 选择一个相对优秀的对象
     *
     * @return 对象
     */
    @Override
    public String pickOneAtLeast() {
        // 创建数组
        final Enumeration<FaultItem> elements = this.faultItemTable.elements();
        List<FaultItem> tmpList = new LinkedList<>();
        // 枚举类的迭代对象，新版本弃用，效率不如迭代器
        // 获取本地故障列表中的所有元素到tmpList中
        while (elements.hasMoreElements()) {
            final FaultItem faultItem = elements.nextElement();
            tmpList.add(faultItem);
        }
        if (!tmpList.isEmpty()) {
            // 打乱 + 排序。TODO 疑问：应该只能二选一。猜测Collections.shuffle(tmpList)去掉。
            // 打乱, 给每个元素一个随机值
            Collections.shuffle(tmpList);
            // 排序：可用性 > 延迟 > 开始可用时间
            // 从最优在前，进行排序
            Collections.sort(tmpList);
            // 选择顺序在前一半的对象
            final int half = tmpList.size() / 2;
            if (half <= 0) {
                // 中位的标记如果小于等于0，说明就一个故障信息，就获取第一个故障信息对象名
                return tmpList.get(0).getName();
            } else {
                // 根据whichItemWorst + 1获取所有故障信息中的除余过的故障元素的对象名称(根据排序过的故障信息，挨个返回)
                final int i = this.whichItemWorst.getAndIncrement() % half;
                return tmpList.get(i).getName();
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "LatencyFaultToleranceImpl{" +
            "faultItemTable=" + faultItemTable +
            ", whichItemWorst=" + whichItemWorst +
            '}';
    }

    /**
     * 对象故障信息。
     * 维护对象的名字、延迟、开始可用的时间。
     */
    class FaultItem implements Comparable<FaultItem> {
        /**
         * 对象名
         */
        private final String name;
        /**
         * 延迟
         */
        private volatile long currentLatency;
        /**
         * 开始可用时间
         */
        private volatile long startTimestamp;

        public FaultItem(final String name) {
            this.name = name;
        }

        /**
         * 比较对象
         * 可用性 > 延迟 > 开始可用时间
         *
         * @param other other
         * @return 升序
         */
        @Override
        public int compareTo(final FaultItem other) {
            if (this.isAvailable() != other.isAvailable()) {
                if (this.isAvailable())
                    return -1;

                if (other.isAvailable())
                    return 1;
            }

            if (this.currentLatency < other.currentLatency)
                return -1;
            else if (this.currentLatency > other.currentLatency) {
                return 1;
            }

            if (this.startTimestamp < other.startTimestamp)
                return -1;
            else if (this.startTimestamp > other.startTimestamp) {
                return 1;
            }

            return 0;
        }

        /**
         * 是否可用：当开始可用时间大于当前时间
         *
         * @return 是否可用
         */
        public boolean isAvailable() {
            return (System.currentTimeMillis() - startTimestamp) >= 0;
        }

        @Override
        public int hashCode() {
            int result = getName() != null ? getName().hashCode() : 0;
            result = 31 * result + (int) (getCurrentLatency() ^ (getCurrentLatency() >>> 32));
            result = 31 * result + (int) (getStartTimestamp() ^ (getStartTimestamp() >>> 32));
            return result;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof FaultItem))
                return false;

            final FaultItem faultItem = (FaultItem) o;

            if (getCurrentLatency() != faultItem.getCurrentLatency())
                return false;
            if (getStartTimestamp() != faultItem.getStartTimestamp())
                return false;
            return getName() != null ? getName().equals(faultItem.getName()) : faultItem.getName() == null;

        }

        @Override
        public String toString() {
            return "FaultItem{" +
                "name='" + name + '\'' +
                ", currentLatency=" + currentLatency +
                ", startTimestamp=" + startTimestamp +
                '}';
        }

        public String getName() {
            return name;
        }

        public long getCurrentLatency() {
            return currentLatency;
        }

        public void setCurrentLatency(final long currentLatency) {
            this.currentLatency = currentLatency;
        }

        public long getStartTimestamp() {
            return startTimestamp;
        }

        public void setStartTimestamp(final long startTimestamp) {
            this.startTimestamp = startTimestamp;
        }

    }
}
