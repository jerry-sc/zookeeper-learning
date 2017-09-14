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

package org.apache.zookeeper.server.quorum.flexible;

import java.util.Set;
import java.util.Map;

import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;

/**
 * 该类负责维护配置的所有节点，它的职责在于判断给定一个集合，判断出该集合能形成一个法定集合，即最重要的方法{@link QuorumVerifier#containsQuorum(Set)}
 *
 * 默认提供了两种衡量方法，一种就是简单的超过半数就算；另一种相对复杂，需要基于权重
 * All quorum validators have to implement a method called
 * containsQuorum, which verifies if a HashSet of server 
 * identifiers constitutes a quorum.
 *
 */
public interface QuorumVerifier {
    long getWeight(long id);
    boolean containsQuorum(Set<Long> set);
    long getVersion();
    void setVersion(long ver);
    Map<Long, QuorumServer> getAllMembers();
    Map<Long, QuorumServer> getVotingMembers();
    Map<Long, QuorumServer> getObservingMembers();
    boolean equals(Object o);
    String toString();
}
