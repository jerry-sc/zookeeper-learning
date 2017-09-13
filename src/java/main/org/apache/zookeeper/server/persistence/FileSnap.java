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

package org.apache.zookeeper.server.persistence;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.common.AtomicFileOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.util.SerializeUtils;

/**
 * 关于快照文件的命名：snapshot.ZXID 该zxid是当前已经提交的最大ZXID来生成的文件名，注意与日志文件命名的方式。
 * 这样做的目的是为了数据恢复
 *
 * 快照的数据格式如下：
 *
 * FileHeader + sessiontimeouts + aclCache + dataTree + val + path
 *
 *
 * FileHeader: magic + version + dbid
 * sessiontimeouts: count + <id, timeout> + ... + ...
 * aclCache: mapSize + <long,list<acl>> + ... + ...
 * dataTree: path + node    所有节点 + /（结尾分隔符）
 * val: checksum
 * path: /
 *
 * This class implements the snapshot interface.
 * it is responsible for storing, serializing
 * and deserializing the right snapshot.
 * and provides access to the snapshots.
 */
public class FileSnap implements SnapShot {
    File snapDir;
    private volatile boolean close = false;
    private static final int VERSION = 2;
    private static final long dbId = -1;
    private static final Logger LOG = LoggerFactory.getLogger(FileSnap.class);
    public final static int SNAP_MAGIC
            = ByteBuffer.wrap("ZKSN".getBytes()).getInt();

    public FileSnap(File snapDir) {
        this.snapDir = snapDir;
    }

    /**
     * deserialize a data tree from the most recent snapshot
     * @return the zxid of the snapshot
     */
    public long deserialize(DataTree dt, Map<Long, Integer> sessions)
            throws IOException {
        // we run through 100 snapshots (not all of them)
        // if we cannot get it running within 100 snapshots
        // we should  give up
        List<File> snapList = findNValidSnapshots(100);
        if (snapList.size() == 0) {
            return -1L;
        }
        File snap = null;
        boolean foundValid = false;
        for (int i = 0, snapListSize = snapList.size(); i < snapListSize; i++) {
            snap = snapList.get(i);
            LOG.info("Reading snapshot " + snap);
            // 持久化到本地的时候，会把文件的checksum信息一同存入，确保文件没有被损坏
            try (InputStream snapIS = new BufferedInputStream(new FileInputStream(snap));
                 CheckedInputStream crcIn = new CheckedInputStream(snapIS, new Adler32())) {
                InputArchive ia = BinaryInputArchive.getArchive(crcIn);
                deserialize(dt, sessions, ia);
                long checkSum = crcIn.getChecksum().getValue();
                long val = ia.readLong("val");
                if (val != checkSum) {
                    throw new IOException("CRC corruption in snapshot :  " + snap);
                }
                // 找到一个有效即停止
                foundValid = true;
                break;
            } catch (IOException e) {
                LOG.warn("problem reading snap file " + snap, e);
            }
        }
        if (!foundValid) {
            throw new IOException("Not able to find valid snapshots in " + snapDir);
        }
        // 根据快照文件名的ID更新当前最新的内存数据库中的事务，该事务ID未必是最新的，之后在事务日志文件中解析，再更新
        dt.lastProcessedZxid = Util.getZxidFromName(snap.getName(), "snapshot");
        return dt.lastProcessedZxid;
    }

    /**
     * deserialize the datatree from an inputarchive
     * @param dt the datatree to be serialized into
     * @param sessions the sessions to be filled up
     * @param ia the input archive to restore from
     * @throws IOException
     */
    public void deserialize(DataTree dt, Map<Long, Integer> sessions,
                            InputArchive ia) throws IOException {
        FileHeader header = new FileHeader();
        header.deserialize(ia, "fileheader");
        if (header.getMagic() != SNAP_MAGIC) {
            throw new IOException("mismatching magic headers "
                    + header.getMagic() +
                    " !=  " + FileSnap.SNAP_MAGIC);
        }
        SerializeUtils.deserializeSnapshot(dt, ia, sessions);
    }

    /**
     * find the most recent snapshot in the database.
     * @return the file containing the most recent snapshot
     */
    public File findMostRecentSnapshot() throws IOException {
        List<File> files = findNValidSnapshots(1);
        if (files.size() == 0) {
            return null;
        }
        return files.get(0);
    }

    /**
     * 找到N个有效的快照文件，关于有效：只是简单的检查，不保证有效，但很大概率有效
     * find the last (maybe) valid n snapshots. this does some 
     * minor checks on the validity of the snapshots. It just
     * checks for / at the end of the snapshot. This does
     * not mean that the snapshot is truly valid but is
     * valid with a high probability. also, the most recent 
     * will be first on the list. 
     * @param n the number of most recent snapshots
     * @return the last n snapshots (the number might be
     * less than n in case enough snapshots are not available).
     * @throws IOException
     */
    private List<File> findNValidSnapshots(int n) throws IOException {
        List<File> files = Util.sortDataDir(snapDir.listFiles(), "snapshot", false);
        int count = 0;
        List<File> list = new ArrayList<File>();
        for (File f : files) {
            // we should catch the exceptions
            // from the valid snapshot and continue
            // until we find a valid one
            try {
                if (Util.isValidSnapshot(f)) {
                    list.add(f);
                    count++;
                    if (count == n) {
                        break;
                    }
                }
            } catch (IOException e) {
                LOG.info("invalid snapshot " + f, e);
            }
        }
        return list;
    }

    /**
     * find the last n snapshots. this does not have
     * any checks if the snapshot might be valid or not
     * @param n the number of most recent snapshots
     * @return the last n snapshots
     * @throws IOException
     */
    public List<File> findNRecentSnapshots(int n) throws IOException {
        List<File> files = Util.sortDataDir(snapDir.listFiles(), "snapshot", false);
        int count = 0;
        List<File> list = new ArrayList<File>();
        for (File f : files) {
            if (count == n)
                break;
            if (Util.getZxidFromName(f.getName(), "snapshot") != -1) {
                count++;
                list.add(f);
            }
        }
        return list;
    }

    /**
     * serialize the datatree and sessions
     * @param dt the datatree to be serialized
     * @param sessions the sessions to be serialized
     * @param oa the output archive to serialize into
     * @param header the header of this snapshot
     * @throws IOException
     */
    protected void serialize(DataTree dt, Map<Long, Integer> sessions,
                             OutputArchive oa, FileHeader header) throws IOException {
        // this is really a programmatic error and not something that can
        // happen at runtime
        if (header == null)
            throw new IllegalStateException(
                    "Snapshot's not open for writing: uninitialized header");
        header.serialize(oa, "fileheader");
        SerializeUtils.serializeSnapshot(dt, oa, sessions);
    }

    /**
     * 将datatree和sessions写入到快照中去
     *
     *  注意这里的{@link AtomicFileOutputStream} 可以立即同步到磁盘
     *
     * serialize the datatree and session into the file snapshot
     * @param dt the datatree to be serialized
     * @param sessions the sessions to be serialized
     * @param snapShot the file to store snapshot into
     * @param fsync sync the file immediately after write 是否立即同步到硬盘中去，耗时操作
     */
    public synchronized void serialize(DataTree dt, Map<Long, Integer> sessions, File snapShot, boolean fsync)
            throws IOException {
        if (!close) {
            try (CheckedOutputStream crcOut =
                         new CheckedOutputStream(new BufferedOutputStream(fsync ? new AtomicFileOutputStream(snapShot) :
                                 new FileOutputStream(snapShot)),
                                 new Adler32())) {
                //CheckedOutputStream cout = new CheckedOutputStream()
                OutputArchive oa = BinaryOutputArchive.getArchive(crcOut);
                FileHeader header = new FileHeader(SNAP_MAGIC, VERSION, dbId);
                serialize(dt, sessions, oa, header);
                long val = crcOut.getChecksum().getValue();
                oa.writeLong(val, "val");
                oa.writeString("/", "path");
                crcOut.flush();
            }
        }
    }

    /**
     * synchronized close just so that if serialize is in place
     * the close operation will block and will wait till serialize
     * is done and will set the close flag
     */
    @Override
    public synchronized void close() throws IOException {
        close = true;
    }

}
