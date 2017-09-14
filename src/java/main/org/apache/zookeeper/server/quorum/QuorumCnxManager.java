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

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.UnresolvedAddressException;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements a connection manager for leader election using TCP. It
 * maintains one connection for every pair of servers. The tricky part is to
 * guarantee that there is exactly one connection for every pair of servers that
 * are operating correctly and that can communicate over the network.
 *
 * If two servers try to start a connection concurrently, then the connection
 * manager uses a very simple tie-breaking mechanism to decide which connection
 * to drop based on the IP addressed of the two parties. 
 *
 * For every peer, the manager maintains a queue of messages to send. If the
 * connection to any particular peer drops, then the sender thread puts the
 * message back on the list. As this implementation currently uses a queue
 * implementation to maintain messages to send to another peer, we add the
 * message to the tail of the queue, thus changing the order of messages.
 * Although this is not a problem for the leader election, it could be a problem
 * when consolidating peer communication. This is to be verified, though.
 *
 */

/**
 * 初始化阶段，首先创建leader选举所需的网络IO层QuorumCnxManager，同时启动对leader选举端口的监听，等待
 * 集群中的其他服务器连接
 */
public class QuorumCnxManager {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumCnxManager.class);

    /*
     * 设置了消息接受队列能够接受的消息数量
     * Maximum capacity of thread queues
     */
    static final int RECV_CAPACITY = 100;
    // Initialized to 1 to prevent sending
    // stale notifications to peers
    static final int SEND_CAPACITY = 1;

    static final int PACKETMAXSIZE = 1024 * 512;
    
    /*
     * Negative counter for observer server ids.
     */

    private long observerCounter = -1;

    /*
     * Protocol identifier used among peers
     */
    public static final long PROTOCOL_VERSION = -65536L;

    /*
     * Max buffer size to be read from the network.
     */
    static public final int maxBuffer = 2048;

    /*
     * Connection time out value in milliseconds 
     */
    private int cnxTO = 5000;

    /*
     * Local IP address
     */
    final QuorumPeer self;


    ////////////////////////////////////////////////// 以下三个队列有一个共同点：都按SID分组形成队列集合，当前服务器会为其他每一个服务器
    ////////////////////////////////////////////////// 创建一个相应队列，互不干扰。例如发送队列，如果集群中除自身外还有8台服务器，那么当前
    ////////////////////////////////////////////////// 服务器会为这8台机器分别创建一个发送队列

    ///  这样做的原因在于，为了能够进行互相投票，集群中所有机器都需要两两建立起网络连接，这样做便于发送消息
    /**
     * 发送器（SendWorker）集合，每个发送器对应一台zookeeper服务器，负责消息发送
     */
    final ConcurrentHashMap<Long, SendWorker> senderWorkerMap;
    /**
     * 消息发送队列，用于保存那些待发送的消息
     */
    final ConcurrentHashMap<Long, ArrayBlockingQueue<ByteBuffer>> queueSendMap;
    /**
     * 最近发送过的消息
     */
    final ConcurrentHashMap<Long, ByteBuffer> lastMessageSent;

    /**
     * 接受队列，存放那些从其他服务器接收到的消息
     * Reception queue
     */
    public final ArrayBlockingQueue<Message> recvQueue;

    ////////////////////////////////////////////////////////////////////////////////////////////////
    /*
     * Object to synchronize access to recvQueue
     */
    private final Object recvQLock = new Object();

    /*
     * Shutdown flag
     */

    volatile boolean shutdown = false;

    /*
      * 投票监听线程
     * Listener thread
     */
    public final Listener listener;

    /*
     * Counter to count worker threads
     */
    private AtomicInteger threadCnt = new AtomicInteger(0);

    /*
     * Socket options for TCP keepalive
     */
    private final boolean tcpKeepAlive = Boolean.getBoolean("zookeeper.tcpKeepAlive");


    static public class Message {
        Message(ByteBuffer buffer, long sid) {
            this.buffer = buffer;
            this.sid = sid;
        }

        ByteBuffer buffer;
        long sid;
    }

    /**
     * 从其他服务器发过来的数据流中解析，该类主要封装了其他服务器建立连接的信息，包括发送方的ID 和 发送的地址IP与端口
     * This class parses the initial identification sent out by peers with their
     * sid & hostname.
     */
    static public class InitialMessage {
        public Long sid;
        public InetSocketAddress electionAddr;

        InitialMessage(Long sid, InetSocketAddress address) {
            this.sid = sid;
            this.electionAddr = address;
        }

        @SuppressWarnings("serial")
        public static class InitialMessageException extends Exception {
            InitialMessageException(String message, Object... args) {
                super(String.format(message, args));
            }
        }

        static public InitialMessage parse(Long protocolVersion, DataInputStream din)
                throws InitialMessageException, IOException {
            Long sid;

            if (protocolVersion != PROTOCOL_VERSION) {
                throw new InitialMessageException(
                        "Got unrecognized protocol version %s", protocolVersion);
            }

            sid = din.readLong();

            // 读取数据长度
            int remaining = din.readInt();
            if (remaining <= 0 || remaining > maxBuffer) {
                throw new InitialMessageException(
                        "Unreasonable buffer length: %s", remaining);
            }

            byte[] b = new byte[remaining];
            int num_read = din.read(b);

            if (num_read != remaining) {
                throw new InitialMessageException(
                        "Read only %s bytes out of %s sent by server %s",
                        num_read, remaining, sid);
            }

            // FIXME: IPv6 is not supported. Using something like Guava's HostAndPort
            //        parser would be good.
            // 解析出发送方的地址
            String addr = new String(b);
            String[] host_port = addr.split(":");

            if (host_port.length != 2) {
                throw new InitialMessageException("Badly formed address: %s", addr);
            }

            int port;
            try {
                port = Integer.parseInt(host_port[1]);
            } catch (NumberFormatException e) {
                throw new InitialMessageException("Bad port number: %s", host_port[1]);
            }

            return new InitialMessage(sid, new InetSocketAddress(host_port[0], port));
        }
    }

    public QuorumCnxManager(QuorumPeer self) {
        this.recvQueue = new ArrayBlockingQueue<Message>(RECV_CAPACITY);
        this.queueSendMap = new ConcurrentHashMap<Long, ArrayBlockingQueue<ByteBuffer>>();
        this.senderWorkerMap = new ConcurrentHashMap<Long, SendWorker>();
        this.lastMessageSent = new ConcurrentHashMap<Long, ByteBuffer>();

        String cnxToValue = System.getProperty("zookeeper.cnxTimeout");
        if (cnxToValue != null) {
            this.cnxTO = Integer.parseInt(cnxToValue);
        }

        this.self = self;

        // Starts listener thread that waits for connection requests 
        listener = new Listener();
        listener.setName("QuorumPeerListener");
    }

    /**
     * Invokes initiateConnection for testing purposes
     *
     * @param sid
     */
    public void testInitiateConnection(long sid) throws Exception {
        LOG.debug("Opening channel to server " + sid);
        Socket sock = new Socket();
        setSockOpts(sock);
        sock.connect(self.getVotingView().get(sid).electionAddr, cnxTO);
        initiateConnection(sock, sid);
    }

    /**
     * 即发送连接请求
     * If this server has initiated the connection, then it gives up on the
     * connection if it loses challenge. Otherwise, it keeps the connection.
     */
    public boolean initiateConnection(Socket sock, Long sid) {
        try {
            // Use BufferedOutputStream to reduce the number of IP packets. This is
            // important for x-DC scenarios.
            BufferedOutputStream buf = new BufferedOutputStream(sock.getOutputStream());
            DataOutputStream dout = new DataOutputStream(buf);

            // Sending id and challenge

            // 这里发送的并不是投票消息，可以理解为连接信息，所有的投票信息都放在了待发送队列中
            // represents protocol version (in other words - message type)
            dout.writeLong(PROTOCOL_VERSION);
            // 发送方ID
            dout.writeLong(self.getId());
            // 写上发送方的ip 与 端口
            String addr = self.getElectionAddress().getHostString() + ":" + self.getElectionAddress().getPort();
            byte[] addr_bytes = addr.getBytes();
            dout.writeInt(addr_bytes.length);
            dout.write(addr_bytes);
            dout.flush();
        } catch (IOException e) {
            LOG.warn("Ignoring exception reading or writing challenge: ", e);
            closeSocket(sock);
            return false;
        }

        // If lost the challenge, then drop the new connection
        if (sid > self.getId()) {
            LOG.info("Have smaller server identifier, so dropping the " +
                    "connection: (" + sid + ", " + self.getId() + ")");
            closeSocket(sock);
            // Otherwise proceed with the connection
        } else {
            // 为这个连接绑定发送者，接收者；发送者会监控待发队列，一旦有数据即发出去。接收者不断从tcp读取消息，并保存到接收队列中
            SendWorker sw = new SendWorker(sock, sid);
            RecvWorker rw = new RecvWorker(sock, sid, sw);
            sw.setRecv(rw);

            SendWorker vsw = senderWorkerMap.get(sid);

            if (vsw != null)
                vsw.finish();

            senderWorkerMap.put(sid, sw);
            queueSendMap.putIfAbsent(sid, new ArrayBlockingQueue<ByteBuffer>(
                    SEND_CAPACITY));

            sw.start();
            rw.start();

            return true;

        }
        return false;
    }


    /**
     * If this server receives a connection request, then it gives up on the new
     * connection if it wins. Notice that it checks whether it has a connection
     * to this server already or not. If it does, then it sends the smallest
     * possible long value to lose the challenge.
     *
     */
    public void receiveConnection(Socket sock) {
        // 发送方id
        Long sid = null, protocolVersion = null;
        // 发送方地址
        InetSocketAddress electionAddr = null;

        try {
            DataInputStream din = new DataInputStream(sock.getInputStream());

            protocolVersion = din.readLong();
            if (protocolVersion >= 0) { // this is a server id and not a protocol version
                sid = protocolVersion;
            } else {
                try {
                    // 解析出发送方的表示器
                    InitialMessage init = InitialMessage.parse(protocolVersion, din);
                    sid = init.sid;
                    electionAddr = init.electionAddr;
                } catch (InitialMessage.InitialMessageException ex) {
                    LOG.error(ex.toString());
                    closeSocket(sock);
                    return;
                }
            }

            if (sid == QuorumPeer.OBSERVER_ID) {
                /*
                 * Choose identifier at random. We need a value to identify
                 * the connection.
                 */

                sid = observerCounter--;
                LOG.info("Setting arbitrary identifier to observer: {}", sid);
            }
        } catch (IOException e) {
            closeSocket(sock);
            LOG.warn("Exception reading or writing challenge: {}", e.toString());
            return;
        }

        //If wins the challenge, then close the new connection.
        // 如果赢了，即自己的SID值比对方的SID大，那么会断开该连接，然后会主动去和远程服务器建立连接。
        if (sid < self.getId()) {
            /*
             * This replica might still believe that the connection to sid is
             * up, so we have to shut down the workers before trying to open a
             * new connection.
             */
            SendWorker sw = senderWorkerMap.get(sid);
            if (sw != null) {
                sw.finish();
            }

            /*
             * Now we start a new connection
             */
            LOG.debug("Create new connection to server: {}", sid);
            closeSocket(sock);

            // 主动建立连接，发送数据
            if (electionAddr != null) {
                connectOne(sid, electionAddr);
            } else {
                connectOne(sid);
            }

        } else { // Otherwise start worker threads to receive data.
            SendWorker sw = new SendWorker(sock, sid);
            RecvWorker rw = new RecvWorker(sock, sid, sw);
            sw.setRecv(rw);

            SendWorker vsw = senderWorkerMap.get(sid);

            if (vsw != null) {
                vsw.finish();
            }

            senderWorkerMap.put(sid, sw);

            queueSendMap.putIfAbsent(sid,
                    new ArrayBlockingQueue<ByteBuffer>(SEND_CAPACITY));

            sw.start();
            rw.start();
        }
    }

    /**
     * 构造待发送消息，装载到待发送队列中去
     * @param sid 发送方的ID
     * Processes invoke this message to queue a message to send. Currently, 
     * only leader election uses it.
     */
    public void toSend(Long sid, ByteBuffer b) {
        /*
         * 如果消息是发送给自己的，为了节省流量，直接生成消息放入消息接受队列
         * If sending message to myself, then simply enqueue it (loopback).
         */
        if (self.getId() == sid) {
            b.position(0);
            addToRecvQueue(new Message(b.duplicate(), sid));
            /*
             * Otherwise send to the corresponding thread to send.
             */
        } else {
             /*
              * 初始化的时候，我们知道，每一个节点为其他节点都建立了不同的发送消息队列
              * 因此当有新的消息到来的时候，把消息放入对应的队列中即可
              * 然后调用底层socket进行发送
              * Start a new connection if doesn't have one already.
              */
            ArrayBlockingQueue<ByteBuffer> bq = new ArrayBlockingQueue<ByteBuffer>(
                    SEND_CAPACITY);
            ArrayBlockingQueue<ByteBuffer> oldq = queueSendMap.putIfAbsent(sid, bq);
            if (oldq != null) {
                addToSendQueue(oldq, b);
            } else {
                addToSendQueue(bq, b);
            }
            // 发送
            connectOne(sid);
        }
    }

    /**
     * 注意这个方法是同步的，所以保证只有一条链接存在
     * 这两个connect方法只负责两个服务器之间是否能建立连接，如果能则建立连接，并不会考虑发送投票信息
     * Try to establish a connection to server with id sid using its electionAddr.
     *
     *  @param sid  server id
     *  @return boolean success indication
     */
    synchronized private boolean connectOne(long sid, InetSocketAddress electionAddr) {
        if (senderWorkerMap.get(sid) != null) {
            LOG.debug("There is a connection already for server " + sid);
            return true;
        }

        Socket sock = null;
        try {
            LOG.debug("Opening channel to server " + sid);
            sock = new Socket();
            setSockOpts(sock);
            sock.connect(electionAddr, cnxTO);
            LOG.debug("Connected to server " + sid);
            initiateConnection(sock, sid);
            return true;
        } catch (UnresolvedAddressException e) {
            // Sun doesn't include the address that causes this
            // exception to be thrown, also UAE cannot be wrapped cleanly
            // so we log the exception in order to capture this critical
            // detail.
            LOG.warn("Cannot open channel to " + sid
                    + " at election address " + electionAddr, e);
            closeSocket(sock);
            throw e;
        } catch (IOException e) {
            LOG.warn("Cannot open channel to " + sid
                            + " at election address " + electionAddr,
                    e);
            closeSocket(sock);
            return false;
        }

    }

    /**
     * 这两个connect方法只负责两个服务器之间是否能建立连接，如果能则建立连接，并不会考虑发送投票信息
     * 该方法确保接收方存在，在上一轮出现过
     * Try to establish a connection to server with id sid.
     *
     *  @param sid  server id 接收方ID
     */
    synchronized void connectOne(long sid) {
        // 检查两个节点之间是否已经建立了连接，如果已经建立，重用即可
        if (senderWorkerMap.get(sid) != null) {
            LOG.debug("There is a connection already for server " + sid);
            return;
        }
        synchronized (self.QV_LOCK) {
            boolean knownId = false;
            // Resolve hostname for the remote server before attempting to
            // connect in case the underlying ip address has changed.
            self.recreateSocketAddresses(sid);

            // 验证该ID是否有效，是否在上一次出现过，即是否在QuorumVerifier中
            Map<Long, QuorumPeer.QuorumServer> lastCommittedView = self.getView();
            QuorumVerifier lastSeenQV = self.getLastSeenQuorumVerifier();
            Map<Long, QuorumPeer.QuorumServer> lastProposedView = lastSeenQV.getAllMembers();
            if (lastCommittedView.containsKey(sid)) {
                knownId = true;
                if (connectOne(sid, lastCommittedView.get(sid).electionAddr))
                    return;
            }
            if (lastSeenQV != null && lastProposedView.containsKey(sid)
                    && (!knownId || (lastProposedView.get(sid).electionAddr !=
                    lastCommittedView.get(sid).electionAddr))) {
                knownId = true;
                if (connectOne(sid, lastProposedView.get(sid).electionAddr))
                    return;
            }
            if (!knownId) {
                LOG.warn("Invalid server id: " + sid);
                return;
            }
        }
    }


    /**
     * Try to establish a connection with each server if one
     * doesn't exist.
     */
    public void connectAll() {
        long sid;
        for (Enumeration<Long> en = queueSendMap.keys();
             en.hasMoreElements(); ) {
            sid = en.nextElement();
            connectOne(sid);
        }
    }


    /**
     * Check if all queues are empty, indicating that all messages have been delivered.
     */
    boolean haveDelivered() {
        for (ArrayBlockingQueue<ByteBuffer> queue : queueSendMap.values()) {
            LOG.debug("Queue size: " + queue.size());
            if (queue.size() == 0) {
                return true;
            }
        }

        return false;
    }

    /**
     * Flag that it is time to wrap up all activities and interrupt the listener.
     */
    public void halt() {
        shutdown = true;
        LOG.debug("Halting listener");
        listener.halt();

        // Wait for the listener to terminate.
        try {
            listener.join();
        } catch (InterruptedException ex) {
            LOG.warn("Got interrupted before joining the listener", ex);
        }
        softHalt();
    }

    /**
     * A soft halt simply finishes workers.
     */
    public void softHalt() {
        for (SendWorker sw : senderWorkerMap.values()) {
            LOG.debug("Halting sender: " + sw);
            sw.finish();
        }
    }

    /**
     * Helper method to set socket options.
     *
     * @param sock
     *            Reference to socket
     */
    private void setSockOpts(Socket sock) throws SocketException {
        sock.setTcpNoDelay(true);
        // socket底层会在一定时间间隔内发送心跳，判断是否双方还连接（默认2小时）
        sock.setKeepAlive(tcpKeepAlive);
        // 该方法指，在inputstream里 如果超过指定时间还没有接受到数据，那么抛出异常，需要注意的是，只是在inputstream中读取才有效，即read方法有效
        // 此外，此方法只有一次有效，即如果第一次在指定时间内读到了数据，之后长时间没读到，也不会报错
        // 设置超时时间，为集群间心跳的最大延时时间，默认为tickTime的5倍
        sock.setSoTimeout(self.tickTime * self.syncLimit);
    }

    /**
     * Helper method to close a socket.
     *
     * @param sock
     *            Reference to socket
     */
    private void closeSocket(Socket sock) {
        if (sock == null) {
            return;
        }

        try {
            sock.close();
        } catch (IOException ie) {
            LOG.error("Exception while closing", ie);
        }
    }

    /**
     * Return number of worker threads
     */
    public long getThreadCount() {
        return threadCnt.get();
    }

    /**
     * Return reference to QuorumPeer
     */
    public QuorumPeer getQuorumPeer() {
        return self;
    }

    /**
     * 监听投票端口，接受其他服务器的投票信息
     * Thread to listen on some port
     */
    public class Listener extends ZooKeeperThread {

        /**
         * 投票用的是原始的socket，并没有用非阻塞IO
         */
        volatile ServerSocket ss = null;

        public Listener() {
            // During startup of thread, thread name will be overridden to
            // specific election address
            super("ListenerThread");
        }

        /**
         * Sleeps on accept().
         */
        @Override
        public void run() {
            int numRetries = 0;
            InetSocketAddress addr;
            Socket client = null;
            while ((!shutdown) && (numRetries < 3)) {
                try {
                    ss = new ServerSocket();
                    ss.setReuseAddress(true);
                    if (self.getQuorumListenOnAllIPs()) {
                        int port = self.getElectionAddress().getPort();
                        addr = new InetSocketAddress(port);
                    } else {
                        // Resolve hostname for this server in case the
                        // underlying ip address has changed.
                        self.recreateSocketAddresses(self.getId());
                        addr = self.getElectionAddress();
                    }
                    LOG.info("My election bind port: " + addr.toString());
                    setName(addr.toString());
                    ss.bind(addr);
                    while (!shutdown) {
                        try {
                            client = ss.accept();
                            setSockOpts(client);
                            LOG.info("Received connection request "
                                    + client.getRemoteSocketAddress());

                            // 处理新连接
                            receiveConnection(client);
                            numRetries = 0;
                        } catch (SocketTimeoutException e) {
                            LOG.warn("The socket is listening for the election accepted "
                                    + "and it timed out unexpectedly, but will retry."
                                    + "see ZOOKEEPER-2836");
                        }
                    }
                } catch (IOException e) {
                    if (shutdown) {
                        break;
                    }
                    LOG.error("Exception while listening", e);
                    numRetries++;
                    try {
                        ss.close();
                        Thread.sleep(1000);
                    } catch (IOException ie) {
                        LOG.error("Error closing server socket", ie);
                    } catch (InterruptedException ie) {
                        LOG.error("Interrupted while sleeping. " +
                                "Ignoring exception", ie);
                    }
                    closeSocket(client);
                }
            }
            LOG.info("Leaving listener");
            if (!shutdown) {
                LOG.error("As I'm leaving the listener thread, "
                        + "I won't be able to participate in leader "
                        + "election any longer: "
                        + self.getElectionAddress());
            } else if (ss != null) {
                // Clean up for shutdown.
                try {
                    ss.close();
                } catch (IOException ie) {
                    // Don't log an error for shutdown.
                    LOG.debug("Error closing server socket", ie);
                }
            }
        }

        /**
         * Halts this listener thread.
         */
        void halt() {
            try {
                LOG.debug("Trying to close listener: " + ss);
                if (ss != null) {
                    LOG.debug("Closing listener: " + self.getId());
                    ss.close();
                }
            } catch (IOException e) {
                LOG.warn("Exception when shutting down listener: " + e);
            }
        }
    }

    /**
     * Thread to send messages. Instance waits on a queue, and send a message as
     * soon as there is one available. If connection breaks, then opens a new
     * one.
     */
    class SendWorker extends ZooKeeperThread {
        Long sid;
        Socket sock;
        RecvWorker recvWorker;
        volatile boolean running = true;
        DataOutputStream dout;

        /**
         * An instance of this thread receives messages to send
         * through a queue and sends them to the server sid.
         *
         * @param sock
         *            Socket to remote peer
         * @param sid
         *            Server identifier of remote peer
         */
        SendWorker(Socket sock, Long sid) {
            super("SendWorker:" + sid);
            this.sid = sid;
            this.sock = sock;
            recvWorker = null;
            try {
                dout = new DataOutputStream(sock.getOutputStream());
            } catch (IOException e) {
                LOG.error("Unable to access socket output stream", e);
                closeSocket(sock);
                running = false;
            }
            LOG.debug("Address of remote peer: " + this.sid);
        }

        synchronized void setRecv(RecvWorker recvWorker) {
            this.recvWorker = recvWorker;
        }

        /**
         * Returns RecvWorker that pairs up with this SendWorker.
         *
         * @return RecvWorker
         */
        synchronized RecvWorker getRecvWorker() {
            return recvWorker;
        }

        synchronized boolean finish() {
            LOG.debug("Calling finish for " + sid);

            if (!running) {
                /*
                 * Avoids running finish() twice. 
                 */
                return running;
            }

            running = false;
            closeSocket(sock);

            this.interrupt();
            if (recvWorker != null) {
                recvWorker.finish();
            }

            LOG.debug("Removing entry from senderWorkerMap sid=" + sid);

            senderWorkerMap.remove(sid, this);
            threadCnt.decrementAndGet();
            return running;
        }

        /**
         * 将消息发送给socket另一端
         */
        synchronized void send(ByteBuffer b) throws IOException {
            byte[] msgBytes = new byte[b.capacity()];
            try {
                b.position(0);
                b.get(msgBytes);
            } catch (BufferUnderflowException be) {
                LOG.error("BufferUnderflowException ", be);
                return;
            }
            dout.writeInt(b.capacity());
            dout.write(b.array());
            dout.flush();
        }

        @Override
        public void run() {
            threadCnt.incrementAndGet();
            try {
                /**
                 * 一旦zookeeper发现发送队列为空，这个时候要从lastMessageSent取出一个最近发送过的消息
                 * 来进行再次发送。这样做是为了：接收方在消息接收前或者接收到消息后服务器挂了，导致消息未被正确处理。
                 * 无须担心服务器对重复消息的处理
                 *
                 * If there is nothing in the queue to send, then we
                 * send the lastMessage to ensure that the last message
                 * was received by the peer. The message could be dropped
                 * in case self or the peer shutdown their connection
                 * (and exit the thread) prior to reading/processing
                 * the last message. Duplicate messages are handled correctly
                 * by the peer.
                 *
                 * If the send queue is non-empty, then we have a recent
                 * message than that stored in lastMessage. To avoid sending
                 * stale message, we should send the message in the send queue.
                 */
                ArrayBlockingQueue<ByteBuffer> bq = queueSendMap.get(sid);
                if (bq == null || isSendQueueEmpty(bq)) {
                    ByteBuffer b = lastMessageSent.get(sid);
                    if (b != null) {
                        LOG.debug("Attempting to send lastMessage to sid=" + sid);
                        send(b);
                    }
                }
            } catch (IOException e) {
                LOG.error("Failed to send last message. Shutting down thread.", e);
                this.finish();
            }

            try {
                while (running && !shutdown && sock != null) {

                    ByteBuffer b = null;
                    try {
                        ArrayBlockingQueue<ByteBuffer> bq = queueSendMap
                                .get(sid);
                        if (bq != null) {
                            b = pollSendQueue(bq, 1000, TimeUnit.MILLISECONDS);
                        } else {
                            LOG.error("No queue of incoming messages for " +
                                    "server " + sid);
                            break;
                        }

                        if (b != null) {
                            // 记录上一个发送的消息
                            lastMessageSent.put(sid, b);
                            send(b);
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted while waiting for message on queue",
                                e);
                    }
                }
            } catch (Exception e) {
                LOG.warn("Exception when using channel: for id " + sid + " my id = " +
                        self.getId() + " error = " + e);
            }
            this.finish();
            LOG.warn("Send worker leaving thread " + " id " + sid + " my id = " + self.getId());
        }
    }

    /**
     * 从tcp连接中不断读取发送过来的投票信息
     * Thread to receive messages. Instance waits on a socket read. If the
     * channel breaks, then removes itself from the pool of receivers.
     */
    class RecvWorker extends ZooKeeperThread {
        Long sid;
        Socket sock;
        volatile boolean running = true;
        DataInputStream din;
        final SendWorker sw;

        RecvWorker(Socket sock, Long sid, SendWorker sw) {
            super("RecvWorker:" + sid);
            this.sid = sid;
            this.sock = sock;
            this.sw = sw;
            try {
                din = new DataInputStream(sock.getInputStream());
                // 一直等待，知道数据的到来
                // OK to wait until socket disconnects while reading.
                sock.setSoTimeout(0);
            } catch (IOException e) {
                LOG.error("Error while accessing socket for " + sid, e);
                closeSocket(sock);
                running = false;
            }
        }

        /**
         * Shuts down this worker
         *
         * @return boolean  Value of variable running
         */
        synchronized boolean finish() {
            if (!running) {
                /*
                 * Avoids running finish() twice. 
                 */
                return running;
            }
            running = false;

            this.interrupt();
            threadCnt.decrementAndGet();
            return running;
        }

        @Override
        public void run() {
            threadCnt.incrementAndGet();
            try {
                while (running && !shutdown && sock != null) {
                    /**
                     * 读取投票消息的长度
                     * Reads the first int to determine the length of the
                     * message
                     */
                    int length = din.readInt();
                    if (length <= 0 || length > PACKETMAXSIZE) {
                        throw new IOException(
                                "Received packet with invalid packet: "
                                        + length);
                    }
                    /**
                     * Allocates a new ByteBuffer to receive the message
                     */
                    byte[] msgArray = new byte[length];
                    din.readFully(msgArray, 0, length);
                    ByteBuffer message = ByteBuffer.wrap(msgArray);
                    // 将读取到的消息放入接收队列中
                    addToRecvQueue(new Message(message.duplicate(), sid));
                }
            } catch (Exception e) {
                LOG.warn("Connection broken for id " + sid + ", my id = " +
                        self.getId() + ", error = ", e);
            } finally {
                LOG.warn("Interrupting SendWorker");
                sw.finish();
                closeSocket(sock);
            }
        }
    }

    /**
     * Inserts an element in the specified queue. If the Queue is full, this
     * method removes an element from the head of the Queue and then inserts
     * the element at the tail. It can happen that the an element is removed
     * by another thread in {@link SendWorker#processMessage() processMessage}
     * method before this method attempts to remove an element from the queue.
     * This will cause {@link ArrayBlockingQueue#remove() remove} to throw an
     * exception, which is safe to ignore.
     *
     * Unlike {@link #addToRecvQueue(Message) addToRecvQueue} this method does
     * not need to be synchronized since there is only one thread that inserts
     * an element in the queue and another thread that reads from the queue.
     *
     * @param queue
     *          Reference to the Queue
     * @param buffer
     *          Reference to the buffer to be inserted in the queue
     */
    private void addToSendQueue(ArrayBlockingQueue<ByteBuffer> queue,
                                ByteBuffer buffer) {
        if (queue.remainingCapacity() == 0) {
            try {
                queue.remove();
            } catch (NoSuchElementException ne) {
                // element could be removed by poll()
                LOG.debug("Trying to remove from an empty " +
                        "Queue. Ignoring exception " + ne);
            }
        }
        try {
            queue.add(buffer);
        } catch (IllegalStateException ie) {
            // This should never happen
            LOG.error("Unable to insert an element in the queue " + ie);
        }
    }

    /**
     * Returns true if queue is empty.
     * @param queue
     *          Reference to the queue
     * @return
     *      true if the specified queue is empty
     */
    private boolean isSendQueueEmpty(ArrayBlockingQueue<ByteBuffer> queue) {
        return queue.isEmpty();
    }

    /**
     * Retrieves and removes buffer at the head of this queue,
     * waiting up to the specified wait time if necessary for an element to
     * become available.
     *
     * {@link ArrayBlockingQueue#poll(long, java.util.concurrent.TimeUnit)}
     */
    private ByteBuffer pollSendQueue(ArrayBlockingQueue<ByteBuffer> queue,
                                     long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    /**
     * Inserts an element in the {@link #recvQueue}. If the Queue is full, this
     * methods removes an element from the head of the Queue and then inserts
     * the element at the tail of the queue.
     *
     * This method is synchronized to achieve fairness between two threads that
     * are trying to insert an element in the queue. Each thread checks if the
     * queue is full, then removes the element at the head of the queue, and
     * then inserts an element at the tail. This three-step process is done to
     * prevent a thread from blocking while inserting an element in the queue.
     * If we do not synchronize the call to this method, then a thread can grab
     * a slot in the queue created by the second thread. This can cause the call
     * to insert by the second thread to fail.
     * Note that synchronizing this method does not block another thread
     * from polling the queue since that synchronization is provided by the
     * queue itself.
     *
     * @param msg
     *          Reference to the message to be inserted in the queue
     */
    public void addToRecvQueue(Message msg) {
        synchronized (recvQLock) {
            // 由于限制了接受队列的数量，所以需要检查是否还有空间
            if (recvQueue.remainingCapacity() == 0) {
                try {
                    // 如果队列满了，那么把队头的元素删除，再加入新消息
                    recvQueue.remove();
                } catch (NoSuchElementException ne) {
                    // element could be removed by poll()
                    LOG.debug("Trying to remove from an empty " +
                            "recvQueue. Ignoring exception " + ne);
                }
            }
            try {
                recvQueue.add(msg);
            } catch (IllegalStateException ie) {
                // This should never happen
                LOG.error("Unable to insert element in the recvQueue " + ie);
            }
        }
    }

    /**
     * Retrieves and removes a message at the head of this queue,
     * waiting up to the specified wait time if necessary for an element to
     * become available.
     *
     * {@link ArrayBlockingQueue#poll(long, java.util.concurrent.TimeUnit)}
     */
    public Message pollRecvQueue(long timeout, TimeUnit unit)
            throws InterruptedException {
        return recvQueue.poll(timeout, unit);
    }
}
