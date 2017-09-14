/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.zookeeper.server.quorum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.QuorumCnxManager.Message;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 对应算法id=3， 使用TCP协议进行快速选举
 * Implementation of leader election using TCP. It uses an object of the class
 * QuorumCnxManager to manage connections. Otherwise, the algorithm is push-based
 * as with the other UDP implementations.
 *
 * There are a few parameters that can be tuned to change its behavior. First,
 * finalizeWait determines the amount of time to wait until deciding upon a leader.
 * This is part of the leader election algorithm.
 */
public class FastLeaderElection implements Election {
    private static final Logger LOG = LoggerFactory.getLogger(FastLeaderElection.class);

    /**
     * Determine how much time a process has to wait
     * once it believes that it has reached the end of
     * leader election.
     */
    final static int finalizeWait = 200;


    /**
     * Upper bound on the amount of time between two consecutive
     * notification checks. This impacts the amount of time to get
     * the system up again after long partitions. Currently 60 seconds.
     */

    final static int maxNotificationInterval = 60000;

    /**
     * This value is passed to the methods that check the quorum
     * majority of an established ensemble for those values that
     * should not be taken into account in the comparison 
     * (electionEpoch and zxid). 
     */
    final static int IGNOREVALUE = -1;

    /**
     * Connection manager. Fast leader election uses TCP for
     * communication between peers, and QuorumCnxManager manages
     * such connections.
     */

    QuorumCnxManager manager;


    /**
     * Notifications are messages that let other peers know that
     * a given peer has changed its vote, either because it has
     * joined leader election or because it learned of another
     * peer with higher zxid or same zxid and higher server id
     */
    static public class Notification {
        /*
         * Format version, introduced in 3.4.6
         */

        public final static int CURRENTVERSION = 0x2;
        int version;

        /*
         * Proposed leader
         */
        long leader;

        /*
         * zxid of the proposed leader
         */
        long zxid;

        /*
         * Epoch
         */
        long electionEpoch;

        /*
         * current state of sender
         */
        QuorumPeer.ServerState state;

        /*
         * Address of sender
         */
        long sid;

        QuorumVerifier qv;
        /*
         * epoch of the proposed leader
         */
        long peerEpoch;
    }

    static byte[] dummyData = new byte[0];

    /**
     * Messages that a peer wants to send to other peers.
     * These messages can be both Notifications and Acks
     * of reception of notification.
     */
    static public class ToSend {
        static enum mType {crequest, challenge, notification, ack}

        ToSend(mType type,
               long leader,
               long zxid,
               long electionEpoch,
               ServerState state,
               long sid,
               long peerEpoch,
               byte[] configData) {

            this.leader = leader;
            this.zxid = zxid;
            this.electionEpoch = electionEpoch;
            this.state = state;
            this.sid = sid;
            this.peerEpoch = peerEpoch;
            this.configData = configData;
        }

        /*
         * Proposed leader in the case of notification
         */
        long leader;

        /*
         * id contains the tag for acks, and zxid for notifications
         */
        long zxid;

        /*
         * Epoch 实际是选举轮次
         */
        long electionEpoch;

        /*
         * Current state;
         */
        QuorumPeer.ServerState state;

        /*
          * 接受者ID
         * Address of recipient
         */
        long sid;

        /*
         * 使用的是 QuorumVerifier 的toSting 信息，即集群中所有机器的连接信息
         * Used to send a QuorumVerifier (configuration info)
         */
        byte[] configData = dummyData;

        /*
         * Leader epoch 当前服务器的最大事务ID的epoch
         */
        long peerEpoch;
    }

    /**
     * 选票发送队列，用于保存待发送的选票
     */
    LinkedBlockingQueue<ToSend> sendqueue;

    /**
     * 选票接受队列，用于保存接收到的外部选票
     */
    LinkedBlockingQueue<Notification> recvqueue;

    /**
     * 消息处理器，使用{@link WorkerReceiver} 线程不断从QuorumCnxManager获取来自其他服务器发来的消息，并转化成选票，保存到recvQueue
     * 使用{@link WorkerSender} 线程不断从sendqueue中获取选票，然后传递给底层QuorumCnxManager去，真正发送给其他服务器
     *
     * Multi-threaded implementation of message handler. Messenger
     * implements two sub-classes: WorkReceiver and  WorkSender. The
     * functionality of each is obvious from the name. Each of these
     * spawns a new thread.
     */
    protected class Messenger {

        /**
         * 选票接收器，不断从QuorumCnxManager 中获取出选举消息，并转化选票，保存到receQueue中去.
         * 接受过程中也会投票信息校验，选择接收还是拒绝
         *
         * Receives messages from instance of QuorumCnxManager on
         * method run(), and processes such messages.
         */
        class WorkerReceiver extends ZooKeeperThread {
            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerReceiver(QuorumCnxManager manager) {
                super("WorkerReceiver");
                this.stop = false;
                this.manager = manager;
            }

            public void run() {

                Message response;
                while (!stop) {
                    // Sleeps on receive
                    try {
                        // 从QuorumCnxManager的接收队列中获取其他服务器发过来的投票信息
                        response = manager.pollRecvQueue(3000, TimeUnit.MILLISECONDS);
                        if (response == null) continue;

                        // The current protocol and two previous generations all send at least 28 bytes
                        if (response.buffer.capacity() < 28) {
                            LOG.error("Got a short response: " + response.buffer.capacity());
                            continue;
                        }

                        // this is the backwardCompatibility mode in place before ZK-107
                        // It is for a version of the protocol in which we didn't send peer epoch
                        // With peer epoch and version the message became 40 bytes
                        boolean backCompatibility28 = (response.buffer.capacity() == 28);

                        // this is the backwardCompatibility mode for no version information
                        boolean backCompatibility40 = (response.buffer.capacity() == 40);

                        response.buffer.clear();

                        // Instantiate Notification and set its attributes
                        Notification n = new Notification();

                        int rstate = response.buffer.getInt();
                        long rleader = response.buffer.getLong();
                        long rzxid = response.buffer.getLong();
                        long relectionEpoch = response.buffer.getLong();
                        long rpeerepoch;

                        int version = 0x0;
                        if (!backCompatibility28) {
                            rpeerepoch = response.buffer.getLong();
                            if (!backCompatibility40) {
                                /*
                                 * Version added in 3.4.6
                                 * 都是0x2
                                 */
                                version = response.buffer.getInt();
                            } else {
                                LOG.info("Backward compatibility mode (36 bits), server id: {}", response.sid);
                            }
                        } else {
                            LOG.info("Backward compatibility mode (28 bits), server id: {}", response.sid);
                            rpeerepoch = ZxidUtils.getEpochFromZxid(rzxid);
                        }

                        QuorumVerifier rqv = null;

                        // check if we have a version that includes config. If so extract config info from message.
                        if (version > 0x1) {
                            // 读取集群中的每一个节点信息，即QuorumVerifier toString信息，其实该信息一般就是zoo.cfg配置文件中的完整版集群信息
                            int configLength = response.buffer.getInt();
                            byte b[] = new byte[configLength];

                            response.buffer.get(b);

                            synchronized (self) {
                                try {
                                    // 解析出对方服务器配置文件中的所配置的信息 **** 非常重要，用于重配置
                                    rqv = self.configFromString(new String(b));
                                    QuorumVerifier curQV = self.getQuorumVerifier();
                                    // 对比version 来判断是否需要重配置
                                    if (rqv.getVersion() > curQV.getVersion()) {
                                        LOG.info("{} Received version: {} my version: {}", self.getId(),
                                                Long.toHexString(rqv.getVersion()),
                                                Long.toHexString(self.getQuorumVerifier().getVersion()));
                                        if (self.getPeerState() == ServerState.LOOKING) {
                                            LOG.debug("Invoking processReconfig(), state: {}", self.getServerState());
                                            self.processReconfig(rqv, null, null, false);
                                            if (!rqv.equals(curQV)) {
                                                LOG.info("restarting leader election");
                                                self.shuttingDownLE = true;
                                                self.getElectionAlg().shutdown();

                                                break;
                                            }
                                        } else {
                                            LOG.debug("Skip processReconfig(), state: {}", self.getServerState());
                                        }
                                    }
                                } catch (IOException e) {
                                    LOG.error("Something went wrong while processing config received from {}", response.sid);
                                } catch (ConfigException e) {
                                    LOG.error("Something went wrong while processing config received from {}", response.sid);
                                }
                            }
                        } else {
                            LOG.info("Backward compatibility mode (before reconfig), server id: {}", response.sid);
                        }
                       
                        /*
                         * 如果来自Observer的投票消息，则忽略，将自己投票发过去
                         * If it is from a non-voting server (such as an observer or
                         * a non-voting follower), respond right away.
                         */
                        if (!self.getCurrentAndNextConfigVoters().contains(response.sid)) {
                            Vote current = self.getCurrentVote();
                            QuorumVerifier qv = self.getQuorumVerifier();
                            // 发送自己的投票
                            ToSend notmsg = new ToSend(ToSend.mType.notification,
                                    current.getId(),
                                    current.getZxid(),
                                    logicalclock.get(),
                                    self.getPeerState(),
                                    response.sid,
                                    current.getPeerEpoch(),
                                    qv.toString().getBytes());

                            sendqueue.offer(notmsg);
                        } else {
                            // Receive new message
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Receive new notification message. My id = "
                                        + self.getId());
                            }

                            // State of peer that sent this message
                            QuorumPeer.ServerState ackstate = QuorumPeer.ServerState.LOOKING;
                            switch (rstate) {
                                case 0:
                                    ackstate = QuorumPeer.ServerState.LOOKING;
                                    break;
                                case 1:
                                    ackstate = QuorumPeer.ServerState.FOLLOWING;
                                    break;
                                case 2:
                                    ackstate = QuorumPeer.ServerState.LEADING;
                                    break;
                                case 3:
                                    ackstate = QuorumPeer.ServerState.OBSERVING;
                                    break;
                                default:
                                    continue;
                            }

                            n.leader = rleader;
                            n.zxid = rzxid;
                            n.electionEpoch = relectionEpoch;
                            n.state = ackstate;
                            n.sid = response.sid;
                            n.peerEpoch = rpeerepoch;
                            n.version = version;
                            n.qv = rqv;
                            /*
                             * Print notification info
                             */
                            if (LOG.isInfoEnabled()) {
                                printNotification(n);
                            }

                            /*
                             * 当前节点还处于选举阶段
                             * If this server is looking, then send proposed leader
                             */
                            if (self.getPeerState() == QuorumPeer.ServerState.LOOKING) {
                                // 将接收到的消息放到队列中，等待进一步判断
                                recvqueue.offer(n);

                                /*
                                 * 如果发送方也是looking状态，并且发送方的选举epoch小于当前，所以发送方落后了，那么立即发送当前服务器的选票信息
                                 * Send a notification back if the peer that sent this
                                 * message is also looking and its logical clock is
                                 * lagging behind.
                                 */
                                if ((ackstate == QuorumPeer.ServerState.LOOKING)
                                        && (n.electionEpoch < logicalclock.get())) {
                                    Vote v = getVote();
                                    QuorumVerifier qv = self.getQuorumVerifier();
                                    ToSend notmsg = new ToSend(ToSend.mType.notification,
                                            v.getId(),
                                            v.getZxid(),
                                            logicalclock.get(),
                                            self.getPeerState(),
                                            response.sid,
                                            v.getPeerEpoch(),
                                            qv.toString().getBytes());
                                    sendqueue.offer(notmsg);
                                }
                            } else {
                                /*
                                 * 如果当前服务器不是looking状态，即已经选出了leader，那么忽略这个外部选票，同时将leader信息以投票形式发送出去
                                 * If this server is not looking, but the one that sent the ack
                                 * is looking, then send back what it believes to be the leader.
                                 */
                                Vote current = self.getCurrentVote();
                                if (ackstate == QuorumPeer.ServerState.LOOKING) {
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("Sending new notification. My id ={} recipient={} zxid=0x{} leader={} config version = {}",
                                                self.getId(),
                                                response.sid,
                                                Long.toHexString(current.getZxid()),
                                                current.getId(),
                                                Long.toHexString(self.getQuorumVerifier().getVersion()));
                                    }

                                    QuorumVerifier qv = self.getQuorumVerifier();
                                    ToSend notmsg = new ToSend(
                                            ToSend.mType.notification,
                                            current.getId(),
                                            current.getZxid(),
                                            current.getElectionEpoch(),
                                            self.getPeerState(),
                                            response.sid,
                                            current.getPeerEpoch(),
                                            qv.toString().getBytes());
                                    sendqueue.offer(notmsg);
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted Exception while waiting for new message" +
                                e.toString());
                    }
                }
                LOG.info("WorkerReceiver is down");
            }
        }

        /**
         * 该类只需从sendQueue中获取选票，然后放到QuorumCnxManager中的发送队列即可
         *
         * This worker simply dequeues a message to send and
         * and queues it on the manager's queue.
         */
        class WorkerSender extends ZooKeeperThread {
            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerSender(QuorumCnxManager manager) {
                super("WorkerSender");
                this.stop = false;
                this.manager = manager;
            }

            public void run() {
                while (!stop) {
                    try {
                        // 不断尝试从发送队列中寻找，是否有选票需要发送
                        ToSend m = sendqueue.poll(3000, TimeUnit.MILLISECONDS);
                        if (m == null) continue;

                        process(m);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
                LOG.info("WorkerSender is down");
            }

            /**
             * Called by run() once there is a new message to send.
             *
             * @param m     message to send
             */
            void process(ToSend m) {
                ByteBuffer requestBuffer = buildMsg(m.state.ordinal(),
                        m.leader,
                        m.zxid,
                        m.electionEpoch,
                        m.peerEpoch,
                        m.configData);
                // 转化数据格式后，调用底层socket进行发送给其他节点
                manager.toSend(m.sid, requestBuffer);

            }
        }

        WorkerSender ws;
        WorkerReceiver wr;

        Thread wsThread = null;
        Thread wrThread = null;

        /**
         * Constructor of class Messenger.
         *
         * @param manager   Connection manager
         */
        Messenger(QuorumCnxManager manager) {

            this.ws = new WorkerSender(manager);

            this.wsThread = new Thread(this.ws,
                    "WorkerSender[myid=" + self.getId() + "]");
            this.wsThread.setDaemon(true);

            this.wr = new WorkerReceiver(manager);

            this.wrThread = new Thread(this.wr,
                    "WorkerReceiver[myid=" + self.getId() + "]");
            this.wrThread.setDaemon(true);
        }

        /**
         * Starts instances of WorkerSender and WorkerReceiver
         */
        void start() {
            this.wsThread.start();
            this.wrThread.start();
        }

        /**
         * Stops instances of WorkerSender and WorkerReceiver
         */
        void halt() {
            this.ws.stop = true;
            this.wr.stop = true;
        }

    }

    QuorumPeer self;
    Messenger messenger;

    /**
     * 选举轮次：zookeeper服务器leader选举的轮次
     * 规定所有有效的投票必须在同一轮次中，每一次新一轮投票，会首先对此变量自增
     */
    AtomicLong logicalclock = new AtomicLong(); /* Election instance */


    /**
     * 要选举的SID
     */
    long proposedLeader;
    /**
     * 本机最大事务ID
     */
    long proposedZxid;

    /**
     * 本机最大事务所处的epoch
     */
    long proposedEpoch;


    /**
     * Returns the current vlue of the logical clock counter
     */
    public long getLogicalClock() {
        return logicalclock.get();
    }

    static ByteBuffer buildMsg(int state,
                               long leader,
                               long zxid,
                               long electionEpoch,
                               long epoch) {
        // 正好40字节
        byte requestBytes[] = new byte[40];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send, this is called directly only in tests
         */
        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(0x1);

        return requestBuffer;
    }

    static ByteBuffer buildMsg(int state,
                               long leader,
                               long zxid,
                               long electionEpoch,
                               long epoch,
                               byte[] configData) {
        byte requestBytes[] = new byte[44 + configData.length];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send
         */
        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(Notification.CURRENTVERSION);
        requestBuffer.putInt(configData.length);
        requestBuffer.put(configData);

        return requestBuffer;
    }

    /**
     * Constructor of FastLeaderElection. It takes two parameters, one
     * is the QuorumPeer object that instantiated this object, and the other
     * is the connection manager. Such an object should be created only once
     * by each peer during an instance of the ZooKeeper service.
     *
     * @param self  QuorumPeer that created this object
     * @param manager   Connection manager
     */
    public FastLeaderElection(QuorumPeer self, QuorumCnxManager manager) {
        this.stop = false;
        this.manager = manager;
        starter(self, manager);
    }

    /**
     * This method is invoked by the constructor. Because it is a
     * part of the starting procedure of the object that must be on
     * any constructor of this class, it is probably best to keep as
     * a separate method. As we have a single constructor currently,
     * it is not strictly necessary to have it separate.
     *
     * @param self      QuorumPeer that created this object
     * @param manager   Connection manager
     */
    private void starter(QuorumPeer self, QuorumCnxManager manager) {
        this.self = self;
        proposedLeader = -1;
        proposedZxid = -1;

        // 初始化选票发送接收队列
        sendqueue = new LinkedBlockingQueue<ToSend>();
        recvqueue = new LinkedBlockingQueue<Notification>();

        // 将底层socket管理类与发送接收线程管理
        this.messenger = new Messenger(manager);
    }

    /**
     * This method starts the sender and receiver threads.
     */
    public void start() {
        this.messenger.start();
    }

    private void leaveInstance(Vote v) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("About to leave FLE instance: leader={}, zxid=0x{}, my id={}, my state={}",
                    v.getId(), Long.toHexString(v.getZxid()), self.getId(), self.getPeerState());
        }
        recvqueue.clear();
    }

    public QuorumCnxManager getCnxManager() {
        return manager;
    }

    volatile boolean stop;

    public void shutdown() {
        stop = true;
        proposedLeader = -1;
        proposedZxid = -1;
        LOG.debug("Shutting down connection manager");
        manager.halt();
        LOG.debug("Shutting down messenger");
        messenger.halt();
        LOG.debug("FLE is down");
    }

    /**
     * 一旦选票发送变化，那么发送通知给集群中所有其他能投票的节点
     * Send notifications to all peers upon a change in our vote
     */
    private void sendNotifications() {
        // 为集群中每一个有效的服务器发送投票信息
        for (long sid : self.getCurrentAndNextConfigVoters()) {
            QuorumVerifier qv = self.getQuorumVerifier();
            ToSend notmsg = new ToSend(ToSend.mType.notification,
                    proposedLeader,
                    proposedZxid,
                    logicalclock.get(),
                    QuorumPeer.ServerState.LOOKING,
                    sid,
                    proposedEpoch, qv.toString().getBytes());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sending Notification: " + proposedLeader + " (n.leader), 0x" +
                        Long.toHexString(proposedZxid) + " (n.zxid), 0x" + Long.toHexString(logicalclock.get()) +
                        " (n.round), " + sid + " (recipient), " + self.getId() +
                        " (myid), 0x" + Long.toHexString(proposedEpoch) + " (n.peerEpoch)");
            }
            // 添加到发送队列中去，由于发送线程一直阻塞在队列中，一旦发现有数据发送，变唤醒发送数据
            sendqueue.offer(notmsg);
        }
    }

    private void printNotification(Notification n) {
        LOG.info("Notification: "
                + Long.toHexString(n.version) + " (message format version), "
                + n.leader + " (n.leader), 0x"
                + Long.toHexString(n.zxid) + " (n.zxid), 0x"
                + Long.toHexString(n.electionEpoch) + " (n.round), " + n.state
                + " (n.state), " + n.sid + " (n.sid), 0x"
                + Long.toHexString(n.peerEpoch) + " (n.peerEPoch), "
                + self.getPeerState() + " (my state)"
                + (n.qv != null ? (Long.toHexString(n.qv.getVersion()) + " (n.config version)") : ""));
    }


    /**
     * 关键步骤：选票PK，核心逻辑
     * PK目的在于为了确定当前服务器是否需要变更投票，主要从选举轮次，zxid，sid三个因素来考虑
     *
     * Check if a pair (server id, zxid) succeeds our
     * current vote.
     *
     * @param newId    Server identifier
     * @param newZxid  Last zxid observed by the issuer of this vote
     * @return 是否需要变更选票
     */
    protected boolean totalOrderPredicate(long newId, long newZxid, long newEpoch, long curId, long curZxid, long curEpoch) {
        LOG.debug("id: " + newId + ", proposed id: " + curId + ", zxid: 0x" +
                Long.toHexString(newZxid) + ", proposed zxid: 0x" + Long.toHexString(curZxid));
        if (self.getQuorumVerifier().getWeight(newId) == 0) {
            return false;
        }

        /*
         * 如果满足以下任意一个条件就需要变更选票，所以对于第一票投给自己的，不会更改选票
         *
         * We return true if one of the following three cases hold:
         * 1- New epoch is higher 第一个条件用于收到更新的选举轮次
         * 2- New epoch is the same as current epoch, but new zxid is higher 选举轮次相同，如果事务ID更新
         * 3- New epoch is the same as current epoch, new zxid is the same 事务ID相同，SID更大
         *  as current zxid, but server id is higher.
         */

        return ((newEpoch > curEpoch) ||
                ((newEpoch == curEpoch) &&
                        ((newZxid > curZxid) || ((newZxid == curZxid) && (newId > curId)))));
    }

    /**
     * 统计投票，判断是否有过半集合认可该投票，然后终止选举
     * Termination predicate. Given a set of votes, determines if have
     * sufficient to declare the end of the election round.
     *
     * @param votes
     *            Set of votes 接收到的所有投票集合
     * @param vote
     *            Identifier of the vote received last 下一轮需要投的票
     */
    private boolean termPredicate(HashMap<Long, Vote> votes, Vote vote) {
        SyncedLearnerTracker voteSet = new SyncedLearnerTracker();
        voteSet.addQuorumVerifier(self.getQuorumVerifier());
        if (self.getLastSeenQuorumVerifier() != null
                && self.getLastSeenQuorumVerifier().getVersion() > self
                .getQuorumVerifier().getVersion()) {
            voteSet.addQuorumVerifier(self.getLastSeenQuorumVerifier());
        }

        /*
         * 统计即将选举的节点和 已经在收到的的所有选票 相同个数
         * First make the views consistent. Sometimes peers will have different
         * zxids for a server depending on timing.
         */
        for (Map.Entry<Long, Vote> entry : votes.entrySet()) {
            if (vote.equals(entry.getValue())) {
                voteSet.addAck(entry.getKey());
            }
        }
        /*
         * 判断能否构成大多数
         */
        return voteSet.hasAllQuorums();
    }

    /**
     * In the case there is a leader elected, and a quorum supporting
     * this leader, we have to check if the leader has voted and acked
     * that it is leading. We need this check to avoid that peers keep
     * electing over and over a peer that has crashed and it is no
     * longer leading.
     *
     * @param votes set of votes
     * @param   leader  leader id
     * @param   electionEpoch   epoch id
     */
    private boolean checkLeader(
            HashMap<Long, Vote> votes,
            long leader,
            long electionEpoch) {

        boolean predicate = true;

        /*
         * If everyone else thinks I'm the leader, I must be the leader.
         * The other two checks are just for the case in which I'm not the
         * leader. If I'm not the leader and I haven't received a message
         * from leader stating that it is leading, then predicate is false.
         */

        if (leader != self.getId()) {
            if (votes.get(leader) == null) predicate = false;
            else if (votes.get(leader).getState() != ServerState.LEADING) predicate = false;
        } else if (logicalclock.get() != electionEpoch) {
            predicate = false;
        }

        return predicate;
    }

    /**
     * 更新选票信息
     */
    synchronized void updateProposal(long leader, long zxid, long epoch) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Updating proposal: " + leader + " (newleader), 0x"
                    + Long.toHexString(zxid) + " (newzxid), " + proposedLeader
                    + " (oldleader), 0x" + Long.toHexString(proposedZxid) + " (oldzxid)");
        }
        proposedLeader = leader;
        proposedZxid = zxid;
        proposedEpoch = epoch;
    }

    synchronized public Vote getVote() {
        return new Vote(proposedLeader, proposedZxid, proposedEpoch);
    }

    /**
     * A learning state can be either FOLLOWING or OBSERVING.
     * This method simply decides which one depending on the
     * role of the server.
     *
     * @return ServerState
     */
    private ServerState learningState() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            LOG.debug("I'm a participant: " + self.getId());
            return ServerState.FOLLOWING;
        } else {
            LOG.debug("I'm an observer: " + self.getId());
            return ServerState.OBSERVING;
        }
    }

    /**
     * Returns the initial vote value of server identifier.
     *
     * @return long
     */
    private long getInitId() {
        if (self.getQuorumVerifier().getVotingMembers().containsKey(self.getId()))
            return self.getId();
        else return Long.MIN_VALUE;
    }

    /**
     * Returns initial last logged zxid.
     *
     * @return long
     */
    private long getInitLastLoggedZxid() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT)
            return self.getLastLoggedZxid();
        else return Long.MIN_VALUE;
    }

    /**
     * Returns the initial vote value of the peer epoch.
     *
     * @return long
     */
    private long getPeerEpoch() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT)
            try {
                return self.getCurrentEpoch();
            } catch (IOException e) {
                RuntimeException re = new RuntimeException(e.getMessage());
                re.setStackTrace(e.getStackTrace());
                throw re;
            }
        else return Long.MIN_VALUE;
    }

    /**
     * 每当节点状态变为looking，那么新一轮的投票即触发
     * Starts a new round of leader election. Whenever our QuorumPeer
     * changes its state to LOOKING, this method is invoked, and it
     * sends notifications to all other peers.
     */
    public Vote lookForLeader() throws InterruptedException {
        try {
            self.jmxLeaderElectionBean = new LeaderElectionBean();
            MBeanRegistry.getInstance().register(
                    self.jmxLeaderElectionBean, self.jmxLocalPeerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            self.jmxLeaderElectionBean = null;
        }
        // fast lead election 时间
        if (self.start_fle == 0) {
            self.start_fle = Time.currentElapsedTime();
        }
        try {
            /**
             * 已经收到的选票，这些选票都属于同一轮次
             */
            HashMap<Long, Vote> recvset = new HashMap<Long, Vote>();

            HashMap<Long, Vote> outofelection = new HashMap<Long, Vote>();

            int notTimeout = finalizeWait;

            synchronized (this) {
                logicalclock.incrementAndGet();
                // 初始化第一张投给自己的选票
                updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
            }

            LOG.info("New election. My id =  " + self.getId() +
                    ", proposed zxid=0x" + Long.toHexString(proposedZxid));

            // 这里第一次发送选票，向集群中所有节点发送选票，每张选票都是投给自己
            sendNotifications();

            /*
             * 至此，第一轮投票已经完毕，已经发送给对方
             * Loop in which we exchange notifications until we find a leader
             */
            while ((self.getPeerState() == ServerState.LOOKING) &&
                    (!stop)) {
                /*
                 * Remove next notification from queue, times out after 2 times
                 * the termination time
                 */
                Notification n = recvqueue.poll(notTimeout,
                        TimeUnit.MILLISECONDS);

                /*
                 * Sends more notifications if haven't received enough.
                 * Otherwise processes new notification.
                 */
                if (n == null) {
                    // 是否已经发送，如果已经发送了，但是还没有收到回复，那么重新生成数据，再发送一次
                    if (manager.haveDelivered()) {
                        sendNotifications();
                    } else {
                        // 如果数据还没发送走，可能socket连接问题，那么重新建立连接，再发送
                        manager.connectAll();
                    }

                    /*
                     * 可能由于网络问题，所以再延长一些等待数据
                     * Exponential backoff
                     */
                    int tmpTimeOut = notTimeout * 2;
                    notTimeout = (tmpTimeOut < maxNotificationInterval ?
                            tmpTimeOut : maxNotificationInterval);
                    LOG.info("Notification time out: " + notTimeout);
                } else if (self.getCurrentAndNextConfigVoters().contains(n.sid)) {
                    // 第一次总是投给自己的选票
                    /*
                     * Only proceed if the vote comes from a replica in the current or next
                     * voting view.
                     */
                    switch (n.state) {
                        case LOOKING:
                            if (getInitLastLoggedZxid() == -1) {
                                LOG.debug("Ignoring notification as our zxid is -1");
                                break;
                            }
                            if (n.zxid == -1) {
                                LOG.debug("Ignoring notification from member with -1 zxid" + n.sid);
                                break;
                            }
                            // 处理来自外部投票，会根据选举轮次进行不同的处理
                            // 如果外部投票选举轮次大于内部投票， 那么立即更新自己的选举轮次logicalclock，并且清空所有已经收到的
                            // 投票，然后使用初始化的投票来进行PK以确定是否变更内部投票，最终再将内部投票发送出去
                            // If notification > current, replace and send messages out
                            if (n.electionEpoch > logicalclock.get()) {
                                logicalclock.set(n.electionEpoch);
                                // 清空已经收到的选票
                                recvset.clear();
                                if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch,
                                        getInitId(), getInitLastLoggedZxid(), getPeerEpoch())) {
                                    updateProposal(n.leader, n.zxid, n.peerEpoch);
                                } else {
                                    updateProposal(getInitId(),
                                            getInitLastLoggedZxid(),
                                            getPeerEpoch());
                                }
                                // 更新完选票信息，再次发送
                                sendNotifications();

                                // 外部投票选举轮次小于内部投票
                                // 直接忽略，不做任何处理，因为在workerReceiver中已经将当前的选票发送回去
                            } else if (n.electionEpoch < logicalclock.get()) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("Notification election epoch is smaller than logicalclock. n.electionEpoch = 0x"
                                            + Long.toHexString(n.electionEpoch)
                                            + ", logicalclock=0x" + Long.toHexString(logicalclock.get()));
                                }
                                break;

                                // 如果投票一致，就开始选票PK
                            } else if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch,
                                    proposedLeader, proposedZxid, proposedEpoch)) {
                                updateProposal(n.leader, n.zxid, n.peerEpoch);

                                // 更新完选票信息，再次发送
                                sendNotifications();
                            }

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Adding vote: from=" + n.sid +
                                        ", proposed leader=" + n.leader +
                                        ", proposed zxid=0x" + Long.toHexString(n.zxid) +
                                        ", proposed election epoch=0x" + Long.toHexString(n.electionEpoch));
                            }

                            recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch));

                            if (termPredicate(recvset,
                                    new Vote(proposedLeader, proposedZxid,
                                            logicalclock.get(), proposedEpoch))) {

                                // Verify if there is any change in the proposed leader
                                while ((n = recvqueue.poll(finalizeWait,
                                        TimeUnit.MILLISECONDS)) != null) {
                                    if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch,
                                            proposedLeader, proposedZxid, proposedEpoch)) {
                                        recvqueue.put(n);
                                        break;
                                    }
                                }

                            /*
                             * This predicate is true once we don't read any new
                             * relevant message from the reception queue
                             */
                                if (n == null) {
                                    // 设置节点最终的角色
                                    self.setPeerState((proposedLeader == self.getId()) ?
                                            ServerState.LEADING : learningState());

                                    // 并设置最终的leader投票，如果收到其他looking状态的选票，返回该选票, 告诉对方leader信息
                                    Vote endVote = new Vote(proposedLeader,
                                            proposedZxid, proposedEpoch);
                                    leaveInstance(endVote);
                                    return endVote;
                                }
                            }
                            break;
                        case OBSERVING:
                            LOG.debug("Notification from observer: " + n.sid);
                            break;
                        case FOLLOWING:
                        case LEADING:
                        /*
                         * Consider all notifications from the same epoch
                         * together.
                         */
                            if (n.electionEpoch == logicalclock.get()) {
                                recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch));
                                if (termPredicate(recvset, new Vote(n.leader,
                                        n.zxid, n.electionEpoch, n.peerEpoch, n.state))
                                        && checkLeader(outofelection, n.leader, n.electionEpoch)) {
                                    self.setPeerState((n.leader == self.getId()) ?
                                            ServerState.LEADING : learningState());

                                    Vote endVote = new Vote(n.leader, n.zxid, n.peerEpoch);
                                    leaveInstance(endVote);
                                    return endVote;
                                }
                            }

                        /*
                         * Before joining an established ensemble, verify that
                         * a majority are following the same leader.
                         * Only peer epoch is used to check that the votes come
                         * from the same ensemble. This is because there is at
                         * least one corner case in which the ensemble can be
                         * created with inconsistent zxid and election epoch
                         * info. However, given that only one ensemble can be
                         * running at a single point in time and that each 
                         * epoch is used only once, using only the epoch to 
                         * compare the votes is sufficient.
                         * 
                         * @see https://issues.apache.org/jira/browse/ZOOKEEPER-1732
                         */
                            outofelection.put(n.sid, new Vote(n.leader,
                                    IGNOREVALUE, IGNOREVALUE, n.peerEpoch, n.state));
                            if (termPredicate(outofelection, new Vote(n.leader,
                                    IGNOREVALUE, IGNOREVALUE, n.peerEpoch, n.state))
                                    && checkLeader(outofelection, n.leader, IGNOREVALUE)) {
                                synchronized (this) {
                                    logicalclock.set(n.electionEpoch);
                                    self.setPeerState((n.leader == self.getId()) ?
                                            ServerState.LEADING : learningState());
                                }
                                Vote endVote = new Vote(n.leader, n.zxid, n.peerEpoch);
                                leaveInstance(endVote);
                                return endVote;
                            }
                            break;
                        default:
                            LOG.warn("Notification state unrecoginized: " + n.state
                                    + " (n.state), " + n.sid + " (n.sid)");
                            break;
                    }
                } else {
                    LOG.warn("Ignoring notification from non-cluster member " + n.sid);
                }
            }
            return null;
        } finally {
            try {
                if (self.jmxLeaderElectionBean != null) {
                    MBeanRegistry.getInstance().unregister(
                            self.jmxLeaderElectionBean);
                }
            } catch (Exception e) {
                LOG.warn("Failed to unregister with JMX", e);
            }
            self.jmxLeaderElectionBean = null;
        }
    }
}
