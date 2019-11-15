/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.jdbc;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * For Consistent Hash Algorithm, See:
 * https://www.cs.princeton.edu/courses/archive/fall09/cos518/papers/chash.pdf
 */
public class ConsistentHash {
    private final int MD5_BYTE_LEN = 16;
    private final int HASH_BYTE_LEN = 4;
    private final int REPLICATE_NUM_SHARE_MD5 = MD5_BYTE_LEN / HASH_BYTE_LEN;

    private int replicateNum;

    private TreeMap<Long, String> virtualNodes = new TreeMap<Long, String>();
    public ConsistentHash(Integer replicateNum) {
        this.replicateNum = replicateNum;
    }

    public ConsistentHash(List<String> nodes, int replicateNum)
            throws UnsupportedEncodingException, NoSuchAlgorithmException {
        this.replicateNum = replicateNum;
        for (String node: nodes) {
            addNode(node);
        }
    }

    public void addNode(String node) throws UnsupportedEncodingException, NoSuchAlgorithmException {
        // one digest is 16 bytes, each replicate get hash value from 4 bytes of digest
        // so 4 replicates, i.e.MD5_HAS_HASH_NUM, share one digest
        int md5Num = replicateNum / REPLICATE_NUM_SHARE_MD5;
        for (int i = 0; i < md5Num; i ++) {
            byte[] digest = md5(node + i);
            for (int k = 0; k < REPLICATE_NUM_SHARE_MD5; k ++) {
                long key = hash(digest, k);
                virtualNodes.put(key, node);
            }
        }

        int modReplicateNum = replicateNum % REPLICATE_NUM_SHARE_MD5;
        if (modReplicateNum != 0) {
            byte[] digest = md5(node + md5Num);
            for (int k = 0; k < modReplicateNum; k ++) {
                long key = hash(digest, k);
                virtualNodes.put(key, node);
            }
        }
    }

    private byte[] md5(String input) throws UnsupportedEncodingException, NoSuchAlgorithmException {
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        md5.reset();

        byte[] bytes = input.getBytes("UTF-8");
        md5.update(bytes);

        return md5.digest();
    }

    private long hash(byte[] digest, int number) {
        // get hash value from 4, i.e.HASH_BYTE_LEN, bytes of digest
        // if num is 1, get the 4, 5, 6, 7 byte of digest
        return ((((digest[3 + number * 4] & 0xFF) << 24)) |
          (((digest[2 + number * 4] & 0xFF) << 16)) |
          (((digest[1 + number * 4] & 0xFF) << 8)) |
          (digest[0 + number * 4] & 0xFF)) &
           0xFFFFFFFFL;
    }

    public void removeNode(String node) {
        Iterator<Map.Entry<Long, String>> iter = virtualNodes.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Long, String> entry = iter.next();
            if (entry.getValue() == node) {
                iter.remove();
            }
        }
    }

    /**
     * @param key The key to hash
     * @return The nearest node to the hash(key)
     */
    public String searchNode(String key) throws UnsupportedEncodingException, NoSuchAlgorithmException {
        if (virtualNodes.isEmpty()) {
            return null;
        }

        byte[] digest = md5(key);
        long hashValue = hash(digest, 0);

        Iterator<Map.Entry<Long, String>> iter = virtualNodes.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Long, String> entry = iter.next();
            if (entry.getKey() >= hashValue) {
                return entry.getValue();
            }
        }

        return virtualNodes.firstEntry().getValue();
    }

    public Set<String> getNodes() {
        Set<String> nodes = new TreeSet<String>();
        Iterator<Map.Entry<Long, String>> iter = virtualNodes.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Long, String> entry = iter.next();
            nodes.add(entry.getValue());
        }

        return nodes;
    }
}
