/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.zookeeper.common.Time;

/**
 * 在session管理中有一个分桶管理的机制，所有超时的session会放入该队列，然后NIO中连接超时线程会检查该队列,终止连接
 * ExpiryQueue tracks elements in time sorted fixed duration buckets.
 * It's used by SessionTrackerImpl to expire sessions and NIOServerCnxnFactory
 * to expire connections.
 */
public class ExpiryQueue<E> {

    /**
     * 记录每一个session的上一次超时时间点
     */
    private final ConcurrentHashMap<E, Long> elemMap =
        new ConcurrentHashMap<E, Long>();
    /**
     * 用来根据下次会话超时时间点 来归档会话，便于进行会话管理和超时检查
     * The maximum number of buckets is equal to max timeout/expirationInterval,
     * so the expirationInterval should not be too small compared to the
     * max timeout that this expiry queue needs to maintain.
     */
    private final ConcurrentHashMap<Long, Set<E>> expiryMap =
        new ConcurrentHashMap<Long, Set<E>>();

    /**
     * 下次超时检查的时间点, 肯定是expirationInterval的倍数，以此来做到定时检查
     */
    private final AtomicLong nextExpirationTime = new AtomicLong();

    /**
     * 定时检查时间间隔，默认为tickTime
     */
    private final int expirationInterval;

    /**
     * @param expirationInterval 定时检查的时间间隔，默认为tickTime
     */
    public ExpiryQueue(int expirationInterval) {
        this.expirationInterval = expirationInterval;
        nextExpirationTime.set(roundToNextInterval(Time.currentElapsedTime()));
    }

    /**
     * 计算下次超时时间点，如果采用  nextExpirationTime = currentTime + sessionTimeout 那么需要实时检查，成本太高，
     * 所以zookeeper设计成定时检查策略。每次检查的时间间隔默认为tickTime
     * 所以下次的超时时间为为：  nextExpirationTime = currentTime + sessionTimeout
     *                      nextExpirationTime = (nextExpirationTime / expirationInterval + 1) * expirationInterval
     *
     *             从该公式可以看到，nextExpirationTime 一定是expirationInterval 的整数倍，能够确保在nextExpirationTime一定超时
     *             从而实现了分桶管理策略，在每个时间点（一定是expirationInterval整数倍）检查是否超时
     *
     *             核心思想是：将连续值 转化成了 离散值
     *
     */
    private long roundToNextInterval(long time) {
        return (time / expirationInterval + 1) * expirationInterval;
    }

    /**
     * Removes element from the queue.
     * @param elem  element to remove
     * @return      time at which the element was set to expire, or null if
     *              it wasn't present
     */
    public Long remove(E elem) {
        Long expiryTime = elemMap.remove(elem);
        if (expiryTime != null) {
            Set<E> set = expiryMap.get(expiryTime);
            if (set != null) {
                set.remove(elem);
                // We don't need to worry about removing empty sets,
                // they'll eventually be removed when they expire.
            }
        }
        return expiryTime;
    }

    /**
     * Adds or updates expiration time for element in queue, rounding the
     * timeout to the expiry interval bucketed used by this queue.
     * @param elem     element to add/update
     * @param timeout  timout in milliseconds
     * @return         time at which the element is now set to expire if
     *                 changed, or null if unchanged
     */
    public Long update(E elem, int timeout) {
        Long prevExpiryTime = elemMap.get(elem);
        long now = Time.currentElapsedTime();
        Long newExpiryTime = roundToNextInterval(now + timeout);

        if (newExpiryTime.equals(prevExpiryTime)) {
            // No change, so nothing to update
            return null;
        }

        // First add the elem to the new expiry time bucket in expiryMap.
        Set<E> set = expiryMap.get(newExpiryTime);
        if (set == null) {
            // Construct a ConcurrentHashSet using a ConcurrentHashMap
            set = Collections.newSetFromMap(
                new ConcurrentHashMap<E, Boolean>());
            // Put the new set in the map, but only if another thread
            // hasn't beaten us to it
            Set<E> existingSet = expiryMap.putIfAbsent(newExpiryTime, set);
            if (existingSet != null) {
                set = existingSet;
            }
        }
        set.add(elem);

        // Map the elem to the new expiry time. If a different previous
        // mapping was present, clean up the previous expiry bucket.
        prevExpiryTime = elemMap.put(elem, newExpiryTime);
        if (prevExpiryTime != null && !newExpiryTime.equals(prevExpiryTime)) {
            Set<E> prevSet = expiryMap.get(prevExpiryTime);
            if (prevSet != null) {
                prevSet.remove(elem);
            }
        }
        return newExpiryTime;
    }

    /**
     * 获取下次超时检查的时间
     * @return milliseconds until next expiration time, or 0 if has already past
     */
    public long getWaitTime() {
        long now = Time.currentElapsedTime();
        long expirationTime = nextExpirationTime.get();
        return now < expirationTime ? (expirationTime - now) : 0L;
    }

    /**
     * Remove the next expired set of elements from expireMap. This method needs
     * to be called frequently enough by checking getWaitTime(), otherwise there
     * will be a backlog of empty sets queued up in expiryMap.
     *
     * @return next set of expired elements, or an empty set if none are
     *         ready
     */
    public Set<E> poll() {
        long now = Time.currentElapsedTime();
        long expirationTime = nextExpirationTime.get();
        if (now < expirationTime) {
            return Collections.emptySet();
        }

        Set<E> set = null;
        long newExpirationTime = expirationTime + expirationInterval;
        if (nextExpirationTime.compareAndSet(
              expirationTime, newExpirationTime)) {
            set = expiryMap.remove(expirationTime);
        }
        // 执行到这里，如果发现集合中仍有元素，那么这些一定是超时的连接，否则会经历会话迁移操作，移入到新的分桶中
        if (set == null) {
            return Collections.emptySet();
        }
        return set;
    }

    public void dump(PrintWriter pwriter) {
        pwriter.print("Sets (");
        pwriter.print(expiryMap.size());
        pwriter.print(")/(");
        pwriter.print(elemMap.size());
        pwriter.println("):");
        ArrayList<Long> keys = new ArrayList<Long>(expiryMap.keySet());
        Collections.sort(keys);
        for (long time : keys) {
            Set<E> set = expiryMap.get(time);
            if (set != null) {
                pwriter.print(set.size());
                pwriter.print(" expire at ");
                pwriter.print(Time.elapsedTimeToDate(time));
                pwriter.println(":");
                for (E elem : set) {
                    pwriter.print("\t");
                    pwriter.println(elem.toString());
                }
            }
        }
    }

    /**
     * Returns an unmodifiable view of the expiration time -> elements mapping.
     */
    public Map<Long, Set<E>> getExpiryMap() {
        return Collections.unmodifiableMap(expiryMap);
    }
}

