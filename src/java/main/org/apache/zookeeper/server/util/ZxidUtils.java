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

package org.apache.zookeeper.server.util;

/***
 * ZXID设计：
 *  是一个long类型，64位数字
 *  低32位数字：可以看作是一个简单的单调递增的计数器，针对客户端的每一个事务请求，Leader服务器在产生一个新的事务Proposal的时候，都会+1
 *  高32位数字：代表了Leader周期epoch的编号，每当选举产生一个新的Leader服务器，就会从这个leader服务器上去除其本地日志中最大事务Proposal的ZXID，并从
 *  中解析出epoch，然后+1，作为新的epoch，然后将低32位置0
 */
public class ZxidUtils {

	/**
	 * 获得epoch
	 */
	static public long getEpochFromZxid(long zxid) {
		return zxid >> 32L;
	}

	/**
	 * 获得事务计数
	 */
	static public long getCounterFromZxid(long zxid) {
		return zxid & 0xffffffffL;
	}

	/**
	 * 创建新的ZXID
	 */
	static public long makeZxid(long epoch, long counter) {
		return (epoch << 32L) | (counter & 0xffffffffL);
	}
	static public String zxidToString(long zxid) {
		return Long.toHexString(zxid);
	}
}
