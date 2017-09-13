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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.Writer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.common.StringUtils;
import org.apache.zookeeper.common.ZKConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.apache.zookeeper.common.AtomicFileWritingIdiom;
import org.apache.zookeeper.common.AtomicFileWritingIdiom.OutputStreamStatement;
import org.apache.zookeeper.common.AtomicFileWritingIdiom.WriterStatement;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.flexible.QuorumHierarchical;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.VerifyingFileFactory;

/**
 * 该类用于操作与文件系统中的配置文件，之后再将这些配置信息拷贝到实际运行的服务器中
 */
@InterfaceAudience.Public
public class QuorumPeerConfig {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumPeerConfig.class);
    private static final int UNSET_SERVERID = -1;
    public static final String nextDynamicConfigFileSuffix = ".dynamic.next";

    private static boolean standaloneEnabled = true;
    private static boolean reconfigEnabled = false;

    /**
     * 用于建立SSL连接
     */
    protected InetSocketAddress clientPortAddress;
    protected InetSocketAddress secureClientPortAddress;
    /**
     * 数据快照的目录
     */
    protected File dataDir;
    /**
     * 历史事务日志目录，如果该目录不配置，那么会存在数据快照目录下
     */
    protected File dataLogDir;
    protected String dynamicConfigFileStr = null;
    /**
     * 配置文件目录
     */
    protected String configFileStr = null;
    protected int tickTime = ZooKeeperServer.DEFAULT_TICK_TIME;
    /**
     * 单台客户端与服务器之间的最大并发连接数
     */
    protected int maxClientCnxns = 60;

    /** defaults to -1 if not set explicitly
     *
     * 这两个参数用来限制服务端对客户端的超时时间限制
     * */
    protected int minSessionTimeout = -1;
    /** defaults to -1 if not set explicitly */
    protected int maxSessionTimeout = -1;

    /**
     * localSession 的目的在于提高服务端连接的并发
     * 具体特点请看
     * {@link https://issues.apache.org/jira/browse/ZOOKEEPER-1147}
     */
    protected boolean localSessionsEnabled = false;
    protected boolean localSessionsUpgradingEnabled = false;

    /**
     * 默认为tickTime的10倍，表示leader运行follower在该时间内启动，并完成数据同步
     */
    protected int initLimit;

    /**
     * 默认为tickTime的5倍，表示Leader与Follower之间进行心跳检测的最大延时时间
     */
    protected int syncLimit;
    /**
     * 选举算法
     */
    protected int electionAlg = 3;

    /**
     * 选举端口
     */
    protected int electionPort = 2182;
    protected boolean quorumListenOnAllIPs = false;

    /**
     * 即myid文件中的值
     */
    protected long serverId = UNSET_SERVERID;

    protected QuorumVerifier quorumVerifier = null, lastSeenQuorumVerifier = null;

    /**
     * 配置zookeeper在自动清理的时候需要保留的快照数据文件数量和对应的事务日志文件，不能小于3，因为这些
     * 文件用于数据恢复
     */
    protected int snapRetainCount = 3;

    /**
     * 与snapRetainCount配合使用，用于定时清理
     */
    protected int purgeInterval = 0;
    protected boolean syncEnabled = true;

    protected LearnerType peerType = LearnerType.PARTICIPANT;

    /**
     * 设置最小值，保证数据恢复
     * Minimum snapshot retain count.
     * @see org.apache.zookeeper.server.PurgeTxnLog#purge(File, File, int)
     */
    private final int MIN_SNAP_RETAIN_COUNT = 3;

    @SuppressWarnings("serial")
    public static class ConfigException extends Exception {
        public ConfigException(String msg) {
            super(msg);
        }

        public ConfigException(String msg, Exception e) {
            super(msg, e);
        }
    }

    /**
     * Parse a ZooKeeper configuration file
     * @param path the patch of the configuration file
     * @throws ConfigException error processing configuration
     */
    public void parse(String path) throws ConfigException {
        LOG.info("Reading configuration from: " + path);

        try {
            // 验证文件是否存在
            File configFile = (new VerifyingFileFactory.Builder(LOG)
                    .warnForRelativePath()
                    .failForNonExistingPath()
                    .build()).create(path);

            Properties cfg = new Properties();
            FileInputStream in = new FileInputStream(configFile);
            try {
                cfg.load(in);
                configFileStr = path;
            } finally {
                in.close();
            }

            parseProperties(cfg);
        } catch (IOException e) {
            throw new ConfigException("Error processing " + path, e);
        } catch (IllegalArgumentException e) {
            throw new ConfigException("Error processing " + path, e);
        }

        /**
         * 是否启用动态配置文件, 3.5.0中新添加
         */
        if (dynamicConfigFileStr != null) {
            try {
                Properties dynamicCfg = new Properties();
                FileInputStream inConfig = new FileInputStream(dynamicConfigFileStr);
                try {
                    dynamicCfg.load(inConfig);
                    if (dynamicCfg.getProperty("version") != null) {
                        throw new ConfigException("dynamic file shouldn't have version inside");
                    }

                    String version = getVersionFromFilename(dynamicConfigFileStr);
                    // If there isn't any version associated with the filename,
                    // the default version is 0.
                    if (version != null) {
                        dynamicCfg.setProperty("version", version);
                    }
                } finally {
                    inConfig.close();
                }
                setupQuorumPeerConfig(dynamicCfg, false);

            } catch (IOException e) {
                throw new ConfigException("Error processing " + dynamicConfigFileStr, e);
            } catch (IllegalArgumentException e) {
                throw new ConfigException("Error processing " + dynamicConfigFileStr, e);
            }
            File nextDynamicConfigFile = new File(configFileStr + nextDynamicConfigFileSuffix);
            if (nextDynamicConfigFile.exists()) {
                try {
                    Properties dynamicConfigNextCfg = new Properties();
                    FileInputStream inConfigNext = new FileInputStream(nextDynamicConfigFile);
                    try {
                        dynamicConfigNextCfg.load(inConfigNext);
                    } finally {
                        inConfigNext.close();
                    }
                    boolean isHierarchical = false;
                    for (Entry<Object, Object> entry : dynamicConfigNextCfg.entrySet()) {
                        String key = entry.getKey().toString().trim();
                        if (key.startsWith("group") || key.startsWith("weight")) {
                            isHierarchical = true;
                            break;
                        }
                    }
                    lastSeenQuorumVerifier = createQuorumVerifier(dynamicConfigNextCfg, isHierarchical);
                } catch (IOException e) {
                    LOG.warn("NextQuorumVerifier is initiated to null");
                }
            }
        }
    }

    // This method gets the version from the end of dynamic file name.
    // For example, "zoo.cfg.dynamic.0" returns initial version "0".
    // "zoo.cfg.dynamic.1001" returns version of hex number "0x1001".
    // If a dynamic file name doesn't have any version at the end of file,
    // e.g. "zoo.cfg.dynamic", it returns null.
    public static String getVersionFromFilename(String filename) {
        int i = filename.lastIndexOf('.');
        if (i < 0 || i >= filename.length())
            return null;

        String hexVersion = filename.substring(i + 1);
        try {
            long version = Long.parseLong(hexVersion, 16);
            return Long.toHexString(version);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * 具体的参数解析
     * Parse config from a Properties.
     * @param zkProp Properties to parse from.
     * @throws IOException
     * @throws ConfigException
     */
    public void parseProperties(Properties zkProp)
            throws IOException, ConfigException {
        int clientPort = 0;
        int secureClientPort = 0;
        String clientPortAddress = null;
        String secureClientPortAddress = null;
        VerifyingFileFactory vff = new VerifyingFileFactory.Builder(LOG).warnForRelativePath().build();
        for (Entry<Object, Object> entry : zkProp.entrySet()) {
            String key = entry.getKey().toString().trim();
            String value = entry.getValue().toString().trim();
            if (key.equals("dataDir")) {
                // 也需要验证数据目录必须存在
                dataDir = vff.create(value);
            } else if (key.equals("dataLogDir")) {
                // 也需要验证数据目录必须存在
                dataLogDir = vff.create(value);
            } else if (key.equals("clientPort")) {
                clientPort = Integer.parseInt(value);
            } else if (key.equals("localSessionsEnabled")) {
                localSessionsEnabled = Boolean.parseBoolean(value);
            } else if (key.equals("localSessionsUpgradingEnabled")) {
                localSessionsUpgradingEnabled = Boolean.parseBoolean(value);
            } else if (key.equals("clientPortAddress")) {
                clientPortAddress = value.trim();
            } else if (key.equals("secureClientPort")) {
                secureClientPort = Integer.parseInt(value);
            } else if (key.equals("secureClientPortAddress")) {
                secureClientPortAddress = value.trim();
            } else if (key.equals("tickTime")) {
                tickTime = Integer.parseInt(value);
            } else if (key.equals("maxClientCnxns")) {
                maxClientCnxns = Integer.parseInt(value);
            } else if (key.equals("minSessionTimeout")) {
                minSessionTimeout = Integer.parseInt(value);
            } else if (key.equals("maxSessionTimeout")) {
                maxSessionTimeout = Integer.parseInt(value);
            } else if (key.equals("initLimit")) {
                initLimit = Integer.parseInt(value);
            } else if (key.equals("syncLimit")) {
                syncLimit = Integer.parseInt(value);
            } else if (key.equals("electionAlg")) {
                electionAlg = Integer.parseInt(value);
                if (electionAlg != 1 && electionAlg != 2 && electionAlg != 3) {
                    throw new ConfigException("Invalid electionAlg value. Only 1, 2, 3 are supported.");
                }
            } else if (key.equals("quorumListenOnAllIPs")) {
                quorumListenOnAllIPs = Boolean.parseBoolean(value);
            } else if (key.equals("peerType")) {
                if (value.toLowerCase().equals("observer")) {
                    peerType = LearnerType.OBSERVER;
                } else if (value.toLowerCase().equals("participant")) {
                    peerType = LearnerType.PARTICIPANT;
                } else {
                    throw new ConfigException("Unrecognised peertype: " + value);
                }
            } else if (key.equals("syncEnabled")) {
                syncEnabled = Boolean.parseBoolean(value);
            } else if (key.equals("dynamicConfigFile")) {
                dynamicConfigFileStr = value;
            } else if (key.equals("autopurge.snapRetainCount")) {
                snapRetainCount = Integer.parseInt(value);
            } else if (key.equals("autopurge.purgeInterval")) {
                purgeInterval = Integer.parseInt(value);
            } else if (key.equals("standaloneEnabled")) {
                if (value.toLowerCase().equals("true")) {
                    setStandaloneEnabled(true);
                } else if (value.toLowerCase().equals("false")) {
                    setStandaloneEnabled(false);
                } else {
                    throw new ConfigException("Invalid option " + value + " for standalone mode. Choose 'true' or 'false.'");
                }
            } else if (key.equals("reconfigEnabled")) {
                if (value.toLowerCase().equals("true")) {
                    setReconfigEnabled(true);
                } else if (value.toLowerCase().equals("false")) {
                    setReconfigEnabled(false);
                } else {
                    throw new ConfigException("Invalid option " + value + " for reconfigEnabled flag. Choose 'true' or 'false.'");
                }
            } else if ((key.startsWith("server.") || key.startsWith("group") || key.startsWith("weight")) && zkProp.containsKey("dynamicConfigFile")) {
                throw new ConfigException("parameter: " + key + " must be in a separate dynamic config file");
            } else {
                System.setProperty("zookeeper." + key, value);
            }
        }

        //////////////////////////////////////////////////////   以下是对配置文件中配置的参数进行安全性检查

        // Reset to MIN_SNAP_RETAIN_COUNT if invalid (less than 3)
        // PurgeTxnLog.purge(File, File, int) will not allow to purge less
        // than 3. 保证最小副本为3，防止配置文件乱设置
        if (snapRetainCount < MIN_SNAP_RETAIN_COUNT) {
            LOG.warn("Invalid autopurge.snapRetainCount: " + snapRetainCount
                    + ". Defaulting to " + MIN_SNAP_RETAIN_COUNT);
            snapRetainCount = MIN_SNAP_RETAIN_COUNT;
        }

        if (dataDir == null) {
            throw new IllegalArgumentException("dataDir is not set");
        }
        // 如果事务日志目录不存在，则默认使用数据目录
        if (dataLogDir == null) {
            dataLogDir = dataDir;
        }

        if (clientPort == 0) {
            LOG.info("clientPort is not set");
            if (clientPortAddress != null) {
                throw new IllegalArgumentException("clientPortAddress is set but clientPort is not set");
            }
        } else if (clientPortAddress != null) {
            this.clientPortAddress = new InetSocketAddress(
                    InetAddress.getByName(clientPortAddress), clientPort);
            LOG.info("clientPortAddress is {}", this.clientPortAddress.toString());
        } else {
            this.clientPortAddress = new InetSocketAddress(clientPort);
            LOG.info("clientPortAddress is {}", this.clientPortAddress.toString());
        }

        if (secureClientPort == 0) {
            LOG.info("secureClientPort is not set");
            if (secureClientPortAddress != null) {
                throw new IllegalArgumentException("secureClientPortAddress is set but secureClientPort is not set");
            }
        } else if (secureClientPortAddress != null) {
            this.secureClientPortAddress = new InetSocketAddress(
                    InetAddress.getByName(secureClientPortAddress), secureClientPort);
            LOG.info("secureClientPortAddress is {}", this.secureClientPortAddress.toString());
        } else {
            this.secureClientPortAddress = new InetSocketAddress(secureClientPort);
            LOG.info("secureClientPortAddress is {}", this.secureClientPortAddress.toString());
        }
        if (this.secureClientPortAddress != null) {
            configureSSLAuth();
        }

        if (tickTime == 0) {
            throw new IllegalArgumentException("tickTime is not set");
        }

        minSessionTimeout = minSessionTimeout == -1 ? tickTime * 2 : minSessionTimeout;
        maxSessionTimeout = maxSessionTimeout == -1 ? tickTime * 20 : maxSessionTimeout;

        if (minSessionTimeout > maxSessionTimeout) {
            throw new IllegalArgumentException(
                    "minSessionTimeout must not be larger than maxSessionTimeout");
        }
        // 从3.5.0版本开始之后，出现了动态配置文件，考虑到兼容性，仍需要对之前的配置做支持
        // 可以发现，新版本中，将关于集群扩容方面的属性都放到了动态配置文件中去，所以考虑到兼容性方面（因为以前写在一起）
        // 也要从配置文件中将集群配置的属性解析并验证
        // backward compatibility - dynamic configuration in the same file as
        // static configuration params see writeDynamicConfig()
        if (dynamicConfigFileStr == null) {
            setupQuorumPeerConfig(zkProp, true);
            if (isDistributed() && isReconfigEnabled()) {
                // we don't backup static config for standalone mode.
                // we also don't backup if reconfig feature is disabled.
                backupOldConfig();
            }
        }
    }

    /**
     * Configure SSL authentication only if it is not configured.
     *
     * @throws ConfigException
     *             If authentication scheme is configured but authentication
     *             provider is not configured.
     */
    private void configureSSLAuth() throws ConfigException {
        String sslAuthProp = "zookeeper.authProvider." + System.getProperty(ZKConfig.SSL_AUTHPROVIDER, "x509");
        if (System.getProperty(sslAuthProp) == null) {
            if ("zookeeper.authProvider.x509".equals(sslAuthProp)) {
                System.setProperty("zookeeper.authProvider.x509",
                        "org.apache.zookeeper.server.auth.X509AuthenticationProvider");
            } else {
                throw new ConfigException("No auth provider configured for the SSL authentication scheme '"
                        + System.getProperty(ZKConfig.SSL_AUTHPROVIDER) + "'.");
            }
        }
    }

    /**
     * 在3.5.0版本之后，如果还是按照原来的配置，即把动态配置也写在静态配置文件zoo.cfg中，那么在重配置启用后，会将配置文件分为两个部分
     * 而把原来的静态文件给备份起来，以bak结尾
     * Backward compatibility -- It would backup static config file on bootup
     * if users write dynamic configuration in "zoo.cfg".
     */
    private void backupOldConfig() throws IOException {
        new AtomicFileWritingIdiom(new File(configFileStr + ".bak"), new OutputStreamStatement() {
            @Override
            public void write(OutputStream output) throws IOException {
                InputStream input = null;
                try {
                    input = new FileInputStream(new File(configFileStr));
                    byte[] buf = new byte[1024];
                    int bytesRead;
                    while ((bytesRead = input.read(buf)) > 0) {
                        output.write(buf, 0, bytesRead);
                    }
                } finally {
                    if (input != null) {
                        input.close();
                    }
                }
            }
        });
    }

    /**
     * Writes dynamic configuration file
     */
    public static void writeDynamicConfig(final String dynamicConfigFilename,
                                          final QuorumVerifier qv,
                                          final boolean needKeepVersion)
            throws IOException {

        new AtomicFileWritingIdiom(new File(dynamicConfigFilename), new WriterStatement() {
            @Override
            public void write(Writer out) throws IOException {
                Properties cfg = new Properties();
                cfg.load(new StringReader(
                        qv.toString()));

                List<String> servers = new ArrayList<String>();
                for (Entry<Object, Object> entry : cfg.entrySet()) {
                    String key = entry.getKey().toString().trim();
                    if (!needKeepVersion && key.startsWith("version"))
                        continue;

                    String value = entry.getValue().toString().trim();
                    servers.add(key
                            .concat("=")
                            .concat(value));
                }

                Collections.sort(servers);
                out.write(StringUtils.joinStrings(servers, "\n"));
            }
        });
    }

    /**
     * Edit static config file.
     * If there are quorum information in static file, e.g. "server.X", "group",
     * it will remove them.
     * If it needs to erase client port information left by the old config,
     * "eraseClientPortAddress" should be set true.
     * It should also updates dynamic file pointer on reconfig.
     */
    public static void editStaticConfig(final String configFileStr,
                                        final String dynamicFileStr,
                                        final boolean eraseClientPortAddress)
            throws IOException {
        // Some tests may not have a static config file.
        if (configFileStr == null)
            return;

        File configFile = (new VerifyingFileFactory.Builder(LOG)
                .warnForRelativePath()
                .failForNonExistingPath()
                .build()).create(configFileStr);

        final File dynamicFile = (new VerifyingFileFactory.Builder(LOG)
                .warnForRelativePath()
                .failForNonExistingPath()
                .build()).create(dynamicFileStr);

        final Properties cfg = new Properties();
        FileInputStream in = new FileInputStream(configFile);
        try {
            cfg.load(in);
        } finally {
            in.close();
        }

        new AtomicFileWritingIdiom(new File(configFileStr), new WriterStatement() {
            @Override
            public void write(Writer out) throws IOException {
                for (Entry<Object, Object> entry : cfg.entrySet()) {
                    String key = entry.getKey().toString().trim();

                    if (key.startsWith("server.")
                            || key.startsWith("group")
                            || key.startsWith("weight")
                            || key.startsWith("dynamicConfigFile")
                            || key.startsWith("peerType")
                            || (eraseClientPortAddress
                            && (key.startsWith("clientPort")
                            || key.startsWith("clientPortAddress")))) {
                        // not writing them back to static file
                        continue;
                    }

                    String value = entry.getValue().toString().trim();
                    out.write(key.concat("=").concat(value).concat("\n"));
                }

                // updates the dynamic file pointer
                String dynamicConfigFilePath = PathUtils.normalizeFileSystemPath(dynamicFile.getCanonicalPath());
                out.write("dynamicConfigFile="
                        .concat(dynamicConfigFilePath)
                        .concat("\n"));
            }
        });
    }


    public static void deleteFile(String filename) {
        if (filename == null) return;
        File f = new File(filename);
        if (f.exists()) {
            try {
                f.delete();
            } catch (Exception e) {
                LOG.warn("deleting " + filename + " failed");
            }
        }
    }


    private static QuorumVerifier createQuorumVerifier(Properties dynamicConfigProp, boolean isHierarchical) throws ConfigException {
        if (isHierarchical) {
            return new QuorumHierarchical(dynamicConfigProp);
        } else {
           /*
                默认方式
             * The default QuorumVerifier is QuorumMaj
             */
            //LOG.info("Defaulting to majority quorums");
            return new QuorumMaj(dynamicConfigProp);
        }
    }

    /**
     * 解析集群相关的配置
     * @param prop
     * @param configBackwardCompatibilityMode 是否为兼容考虑
     * @throws IOException
     * @throws ConfigException
     */
    void setupQuorumPeerConfig(Properties prop, boolean configBackwardCompatibilityMode)
            throws IOException, ConfigException {
        quorumVerifier = parseDynamicConfig(prop, electionAlg, true, configBackwardCompatibilityMode);
        setupMyId();
        setupClientPort();
        setupPeerType();
        checkValidity();
    }

    /**
     * 用于解析动态配置，选择法定集合实现，并进行相关配置的校验
     * Parse dynamic configuration file and return
     * quorumVerifier for new configuration.
     * @param dynamicConfigProp Properties to parse from.
     * @param warnings 用于对于目前配置是否提出意见，当且仅当配置的节点为偶数，或者只有两个的时候
     * @throws IOException
     * @throws ConfigException
     */
    public static QuorumVerifier parseDynamicConfig(Properties dynamicConfigProp, int eAlg, boolean warnings,
                                                    boolean configBackwardCompatibilityMode) throws IOException, ConfigException {
        // 是否使用基于分组权重的选择方式，默认不是
        // 检测方式是：是否是group，weight开头
        boolean isHierarchical = false;
        for (Entry<Object, Object> entry : dynamicConfigProp.entrySet()) {
            String key = entry.getKey().toString().trim();
            if (key.startsWith("group") || key.startsWith("weight")) {
                isHierarchical = true;
            } else if (!configBackwardCompatibilityMode && !key.startsWith("server.") && !key.equals("version")) {
                LOG.info(dynamicConfigProp.toString());
                throw new ConfigException("Unrecognised parameter: " + key);
            }
        }

        // 根据isHierarchical属性值来选择哪种quorum实现方式，一共只有两种方式
        QuorumVerifier qv = createQuorumVerifier(dynamicConfigProp, isHierarchical);

        int numParticipators = qv.getVotingMembers().size();
        int numObservers = qv.getObservingMembers().size();
        if (numParticipators == 0) {
            if (!standaloneEnabled) {
                throw new IllegalArgumentException("standaloneEnabled = false then " +
                        "number of participants should be >0");
            }
            if (numObservers > 0) {
                throw new IllegalArgumentException("Observers w/o participants is an invalid configuration");
            }
        } else if (numParticipators == 1 && standaloneEnabled) {
            // HBase currently adds a single server line to the config, for
            // b/w compatibility reasons we need to keep this here. If standaloneEnabled
            // is true, the QuorumPeerMain script will create a standalone server instead
            // of a quorum configuration
            LOG.error("Invalid configuration, only one server specified (ignoring)");
            if (numObservers > 0) {
                throw new IllegalArgumentException("Observers w/o quorum is an invalid configuration");
            }
        } else {
            if (warnings) {
                if (numParticipators <= 2) {
                    LOG.warn("No server failure will be tolerated. " +
                            "You need at least 3 servers.");
                } else if (numParticipators % 2 == 0) {
                    LOG.warn("Non-optimial configuration, consider an odd number of servers.");
                }
            }

            for (QuorumServer s : qv.getVotingMembers().values()) {
                if (s.electionAddr == null)
                    throw new IllegalArgumentException(
                            "Missing election port for server: " + s.id);
            }
        }
        return qv;
    }

    /**
     * 如果是集群式配置，那么在配置文件目录必须有一个myid的文件
     * @throws IOException
     */
    private void setupMyId() throws IOException {
        File myIdFile = new File(dataDir, "myid");
        // standalone server doesn't need myid file.
        if (!myIdFile.isFile()) {
            return;
        }
        BufferedReader br = new BufferedReader(new FileReader(myIdFile));
        String myIdString;
        try {
            myIdString = br.readLine();
        } finally {
            br.close();
        }
        try {
            serverId = Long.parseLong(myIdString);
            MDC.put("myid", myIdString);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("serverid " + myIdString
                    + " is not a number");
        }
    }

    private void setupClientPort() throws ConfigException {
        if (serverId == UNSET_SERVERID) {
            return;
        }
        QuorumServer qs = quorumVerifier.getAllMembers().get(serverId);
        if (clientPortAddress != null && qs != null && qs.clientAddr != null) {
            if ((!clientPortAddress.getAddress().isAnyLocalAddress()
                    && !clientPortAddress.equals(qs.clientAddr)) ||
                    (clientPortAddress.getAddress().isAnyLocalAddress()
                            && clientPortAddress.getPort() != qs.clientAddr.getPort()))
                throw new ConfigException("client address for this server (id = " + serverId +
                        ") in static config file is " + clientPortAddress +
                        " is different from client address found in dynamic file: " + qs.clientAddr);
        }
        if (qs != null && qs.clientAddr != null) clientPortAddress = qs.clientAddr;
    }

    /**
     * 确定当前节点的节点类型
     */
    private void setupPeerType() {
        // Warn about inconsistent peer type
        LearnerType roleByServersList = quorumVerifier.getObservingMembers().containsKey(serverId) ? LearnerType.OBSERVER
                : LearnerType.PARTICIPANT;
        if (roleByServersList != peerType) {
            LOG.warn("Peer type from servers list (" + roleByServersList
                    + ") doesn't match peerType (" + peerType
                    + "). Defaulting to servers list.");

            peerType = roleByServersList;
        }
    }

    /**
     * 检查分布式配置有效性
     */
    public void checkValidity() throws IOException, ConfigException {
        if (isDistributed()) {
            if (initLimit == 0) {
                throw new IllegalArgumentException("initLimit is not set");
            }
            if (syncLimit == 0) {
                throw new IllegalArgumentException("syncLimit is not set");
            }
            if (serverId == UNSET_SERVERID) {
                throw new IllegalArgumentException("myid file is missing");
            }
        }
    }

    public InetSocketAddress getClientPortAddress() {
        return clientPortAddress;
    }

    public InetSocketAddress getSecureClientPortAddress() {
        return secureClientPortAddress;
    }

    public File getDataDir() {
        return dataDir;
    }

    public File getDataLogDir() {
        return dataLogDir;
    }

    public int getTickTime() {
        return tickTime;
    }

    public int getMaxClientCnxns() {
        return maxClientCnxns;
    }

    public int getMinSessionTimeout() {
        return minSessionTimeout;
    }

    public int getMaxSessionTimeout() {
        return maxSessionTimeout;
    }

    public boolean areLocalSessionsEnabled() {
        return localSessionsEnabled;
    }

    public boolean isLocalSessionsUpgradingEnabled() {
        return localSessionsUpgradingEnabled;
    }

    public int getInitLimit() {
        return initLimit;
    }

    public int getSyncLimit() {
        return syncLimit;
    }

    public int getElectionAlg() {
        return electionAlg;
    }

    public int getElectionPort() {
        return electionPort;
    }

    public int getSnapRetainCount() {
        return snapRetainCount;
    }

    public int getPurgeInterval() {
        return purgeInterval;
    }

    public boolean getSyncEnabled() {
        return syncEnabled;
    }

    public QuorumVerifier getQuorumVerifier() {
        return quorumVerifier;
    }

    public QuorumVerifier getLastSeenQuorumVerifier() {
        return lastSeenQuorumVerifier;
    }

    public Map<Long, QuorumServer> getServers() {
        // returns all configuration servers -- participants and observers
        return Collections.unmodifiableMap(quorumVerifier.getAllMembers());
    }

    public long getServerId() {
        return serverId;
    }

    public boolean isDistributed() {
        return quorumVerifier != null && (!standaloneEnabled || quorumVerifier.getVotingMembers().size() > 1);
    }

    public LearnerType getPeerType() {
        return peerType;
    }

    public String getConfigFilename() {
        return configFileStr;
    }

    public Boolean getQuorumListenOnAllIPs() {
        return quorumListenOnAllIPs;
    }

    public static boolean isStandaloneEnabled() {
        return standaloneEnabled;
    }

    public static void setStandaloneEnabled(boolean enabled) {
        standaloneEnabled = enabled;
    }

    public static boolean isReconfigEnabled() {
        return reconfigEnabled;
    }

    public static void setReconfigEnabled(boolean enabled) {
        reconfigEnabled = enabled;
    }

}
