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

package org.apache.hive.jdbc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZooKeeperManager extends JsonMapper {
    private CuratorFramework zkClient = null;

    // Pattern for key1=value1;key2=value2
    private static final Pattern kvPattern = Pattern.compile("([^=;]*)=([^;]*)[;]?");
    private static final Log LOG = LogFactory.getLog(ZooKeeperManager.class);

    public ZooKeeperManager(String zooKeeperEnsemble) {
        zkClient = CuratorFrameworkFactory.builder().connectString(zooKeeperEnsemble)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                zkClient.close();
            }
        }));

        zkClient.getUnhandledErrorListenable().addListener(new UnhandledErrorListener() {
            @Override
            public void unhandledError(String s, Throwable throwable) {
                LOG.error("Fatal Zookeeper error.");
                System.exit(1);
            }
        });

        zkClient.start();
    }

    public CuratorFramework getZkClient() {
        return zkClient;
    }

    public List<String> getChildren(String path) throws ZooKeeperHiveClientException {
        try {
            if (zkClient.checkExists().forPath(path) == null) {
                return new ArrayList<String>();
            } else {
                return zkClient.getChildren().forPath(path);
            }
        } catch (Exception e) {
            throw new ZooKeeperHiveClientException("Unable to getChildren from ZooKeeper", e);
        }
    }

    public <T> void setData(String path, T t) throws ZooKeeperHiveClientException {
        try {
            byte[] bytes = serializer(t);

            if (zkClient.checkExists().forPath(path) == null) {
                zkClient.create().creatingParentsIfNeeded().forPath(path, bytes);
            } else {
                zkClient.setData().forPath(path, bytes);
            }
        } catch (Exception e) {
            throw new ZooKeeperHiveClientException("Unable to setData to ZooKeeper", e);
        }
    }

    public <T> T getData(String path) throws ZooKeeperHiveClientException {
        try {
            if (zkClient.checkExists().forPath(path) == null) {
                return null;
            }
            byte[] bytes = zkClient.getData().forPath(path);
            return deserializer(bytes);
        } catch (Exception e) {
            throw new ZooKeeperHiveClientException("Unable to getData from ZooKeeper", e);
        }
    }

    public Map<String, String> getKVData(String path) throws ZooKeeperHiveClientException {
        String data = getData(path);
        Map<String, String> kvData = new TreeMap<>();

        Matcher matcher = kvPattern.matcher(data);
        while (matcher.find()) {
            if (matcher.group(1) != null && matcher.group(2) != null) {
                kvData.put(matcher.group(1), matcher.group(2));
            }
        }

        return kvData;
    }

    public void watchAddNode(String path, PathChildrenCacheEventHandler handler)
            throws ZooKeeperHiveClientException {
        watchNode(path, handler, Type.CHILD_ADDED);
    }

    public void watchRemoveNode(String path, PathChildrenCacheEventHandler handler)
            throws ZooKeeperHiveClientException {
        watchNode(path, handler, Type.CHILD_REMOVED);
    }

    public void watchNode(String path, PathChildrenCacheEventHandler handler, final Type eventType)
            throws ZooKeeperHiveClientException {
        try {
            PathChildrenCache cache = new PathChildrenCache(zkClient, path, true);
            cache.start();

            PathChildrenCacheListener listener = new PathChildrenCacheListener() {
                @Override
                public void childEvent(CuratorFramework client,
                                       PathChildrenCacheEvent event) throws Exception {
                    ChildData data = event.getData();
                    if (event.getType() == eventType) {
                        handler.handleEvent(data.getPath(), data.getData());
                    }
                }
            };

            cache.getListenable().addListener(listener);
        } catch (Exception e) {
            throw new ZooKeeperHiveClientException("Unable to watchNode from ZooKeeper", e);
        }
    }
}
