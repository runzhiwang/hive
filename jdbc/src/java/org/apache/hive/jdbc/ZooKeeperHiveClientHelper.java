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

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hive.jdbc.Utils.JdbcConnectionParams;
import org.apache.zookeeper.Watcher;

public class ZooKeeperHiveClientHelper {
  public static final Log LOG = LogFactory.getLog(ZooKeeperHiveClientHelper.class.getName());

  private static volatile ZooKeeperManager zkManager = null;

  public static ZooKeeperManager initZkManager(JdbcConnectionParams connParams) throws ZooKeeperHiveClientException {
    synchronized (ZooKeeperHiveClientHelper.class) {
      if (zkManager != null) {
        zkManager = new ZooKeeperManager(connParams.getZooKeeperEnsemble());
      } else {
        throw new ZooKeeperHiveClientException("ZooKeeperManager single instance has already been created");
      }
    }

    return zkManager;
  }

  public static ZooKeeperManager getZkManager() {
    return zkManager;
  }

  /**
   * A no-op watcher class
   */
  public static class DummyWatcher implements Watcher {
    public void process(org.apache.zookeeper.WatchedEvent event) {
    }
  }

  public static void startZooKeeperClient(JdbcConnectionParams connParams) {
    String zooKeeperEnsemble = connParams.getZooKeeperEnsemble();
    CuratorFramework zooKeeperClient =
            CuratorFrameworkFactory.builder().connectString(zooKeeperEnsemble)
                    .retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();
    zooKeeperClient.start();
  }


  /**
   * Resolve to a host:port by connecting to ZooKeeper and picking a host randomly.
   *
   * @param uri
   * @param connParams
   * @return
   * @throws SQLException
   */
  static String getNextServerUriFromZooKeeper(JdbcConnectionParams connParams)
      throws ZooKeeperHiveClientException {
    String zooKeeperEnsemble = connParams.getZooKeeperEnsemble();
    String zooKeeperNamespace =
        connParams.getSessionVars().get(JdbcConnectionParams.ZOOKEEPER_NAMESPACE);
    if ((zooKeeperNamespace == null) || (zooKeeperNamespace.isEmpty())) {
      zooKeeperNamespace = JdbcConnectionParams.ZOOKEEPER_DEFAULT_NAMESPACE;
    }
    List<String> serverHosts;
    Random randomizer = new Random();
    String serverNode;
    CuratorFramework zooKeeperClient =
        CuratorFrameworkFactory.builder().connectString(zooKeeperEnsemble)
            .retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();
    try {
      zooKeeperClient.start();
      serverHosts = zooKeeperClient.getChildren().forPath("/" + zooKeeperNamespace);
      // Remove the znodes we've already tried from this list
      serverHosts.removeAll(connParams.getRejectedHostZnodePaths());
      if (serverHosts.isEmpty()) {
        throw new ZooKeeperHiveClientException(
            "Tried all existing HiveServer2 uris from ZooKeeper.");
      }
      // Now pick a host randomly
      serverNode = serverHosts.get(randomizer.nextInt(serverHosts.size()));
      connParams.setCurrentHostZnodePath(serverNode);
      String serverUri =
          new String(
              zooKeeperClient.getData().forPath("/" + zooKeeperNamespace + "/" + serverNode),
              Charset.forName("UTF-8"));
      LOG.info("Selected HiveServer2 instance with uri: " + serverUri);
      return serverUri;
    } catch (Exception e) {
      throw new ZooKeeperHiveClientException("Unable to read HiveServer2 uri from ZooKeeper", e);
    } finally {
      // Close the client connection with ZooKeeper
      if (zooKeeperClient != null) {
        zooKeeperClient.close();
      }
    }
  }

  /**
   * Resolve to a host:port by connecting to ZooKeeper and picking a host according to consistent hash.
   *
   * @param uri
   * @param connParams
   * @return
   * @throws SQLException
   */
  static String getServerUriFromZooKeeperByConsistentHash(JdbcConnectionParams connParams)
          throws ZooKeeperHiveClientException {
    String zooKeeperEnsemble = connParams.getZooKeeperEnsemble();
    String zooKeeperNamespace =
            connParams.getSessionVars().get(JdbcConnectionParams.ZOOKEEPER_NAMESPACE);
    if ((zooKeeperNamespace == null) || (zooKeeperNamespace.isEmpty())) {
      zooKeeperNamespace = JdbcConnectionParams.ZOOKEEPER_DEFAULT_NAMESPACE;
    }
    List<String> serverHosts;
    Random randomizer = new Random();
    String serverNode;
    CuratorFramework zooKeeperClient =
            CuratorFrameworkFactory.builder().connectString(zooKeeperEnsemble)
                    .retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();
    try {
      zooKeeperClient.start();
      serverHosts = zooKeeperClient.getChildren().forPath("/" + zooKeeperNamespace);
      // Remove the znodes we've already tried from this list
      serverHosts.removeAll(connParams.getRejectedHostZnodePaths());
      if (serverHosts.isEmpty()) {
        throw new ZooKeeperHiveClientException(
                "Tried all existing HiveServer2 uris from ZooKeeper.");
      }
      // Now pick a host randomly
      serverNode = serverHosts.get(randomizer.nextInt(serverHosts.size()));
      connParams.setCurrentHostZnodePath(serverNode);
      String serverUri =
              new String(
                      zooKeeperClient.getData().forPath("/" + zooKeeperNamespace + "/" + serverNode),
                      Charset.forName("UTF-8"));
      LOG.info("Selected HiveServer2 instance with uri: " + serverUri);
      return serverUri;
    } catch (Exception e) {
      throw new ZooKeeperHiveClientException("Unable to read HiveServer2 uri from ZooKeeper", e);
    } finally {
      // Close the client connection with ZooKeeper
      if (zooKeeperClient != null) {
        zooKeeperClient.close();
      }
    }
  }
}
