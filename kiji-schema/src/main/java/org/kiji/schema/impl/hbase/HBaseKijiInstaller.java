/**
 * (c) Copyright 2012 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.schema.impl.hbase;

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.delegation.Priority;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiAlreadyExistsException;
import org.kiji.schema.KijiInstaller;
import org.kiji.schema.KijiInvalidNameException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.hbase.HBaseFactory;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;
import org.kiji.schema.impl.HBaseAdminFactory;
import org.kiji.schema.impl.HTableInterfaceFactory;
import org.kiji.schema.security.KijiSecurityManager;
import org.kiji.schema.util.LockFactory;
import org.kiji.schema.util.ResourceUtils;
import org.kiji.schema.zookeeper.ZooKeeperUtils;

/** Installs or uninstalls Kiji instances from an HBase cluster. */
@ApiAudience.Public
@ApiStability.Evolving
public final class HBaseKijiInstaller implements KijiInstaller {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseKijiInstaller.class);

  /** Unmodifiable empty map. */
  private static final Map<String, String> EMPTY_MAP = ImmutableMap.of();

  /** Constructs a KijiInstaller.  No-arg constructor required for service provider stuff. */
  public HBaseKijiInstaller() {
  }

  @Override
  public void install(KijiURI uri) throws IOException {
    // Install with a null properties file.
    installWithProperties(uri, EMPTY_MAP);
  }

  @Override
  public void installWithPropertiesFile(KijiURI uri, String propertiesFile) throws IOException {
    final Map<String, String> initialProperties = (null == propertiesFile)
        ? EMPTY_MAP
        : HBaseSystemTable.loadPropertiesFromFileToMap(propertiesFile);
    installWithProperties(uri, initialProperties);
  }

  @Override
  public void installWithProperties(
      KijiURI uri, Map<String, String> properties) throws IOException {
    install(uri, HBaseFactory.Provider.get(), properties, new Configuration());
  }

  /**
   * Installs the specified Kiji instance.
   *
   * @param uri URI of the Kiji instance to install.
   * @param conf Hadoop configuration.
   * @throws java.io.IOException on I/O error.
   * @throws org.kiji.schema.KijiInvalidNameException if the Kiji instance name is invalid or
   *     already exists.
   */
  public void install(KijiURI uri, Configuration conf) throws IOException {
    install(uri, HBaseFactory.Provider.get(), EMPTY_MAP, conf);
  }

  @Override
  public void uninstall(KijiURI uri) throws IOException {
    uninstall(uri, new Configuration());
  }

  /**
   * Uninstalls the specified Kiji instance.
   *
   * @param uri URI of the Kiji instance to uninstall.
   * @param conf Hadoop configuration.
   * @throws java.io.IOException on I/O error.
   * @throws org.kiji.schema.KijiInvalidNameException if the instance name is invalid or already
   *     exists.
   */
  public void uninstall(KijiURI uri, Configuration conf) throws IOException {
    uninstall(uri, HBaseFactory.Provider.get(), conf);
  }

  /**
   * Installs a Kiji instance.
   *
   * @param uri URI of the Kiji instance to install.
   * @param hbaseFactory Factory for HBase instances.
   * @param properties Map of the initial system properties for installation, to be used in addition
   *     to the defaults.
   * @param conf Hadoop configuration.
   * @throws java.io.IOException on I/O error.
   * @throws org.kiji.schema.KijiInvalidNameException if the instance name is invalid or already
   *     exists.
   */
  public void install(
      KijiURI uri,
      HBaseFactory hbaseFactory,
      Map<String, String> properties,
      Configuration conf)
      throws IOException {
    if (uri.getInstance() == null) {
      throw new KijiInvalidNameException(String.format(
          "Kiji URI '%s' does not specify a Kiji instance name", uri));
    }

    final HBaseAdminFactory adminFactory = hbaseFactory.getHBaseAdminFactory(uri);
    final HTableInterfaceFactory tableFactory = hbaseFactory.getHTableInterfaceFactory(uri);
    final LockFactory lockFactory = hbaseFactory.getLockFactory(uri, conf);

    // TODO: Factor this in HBaseKiji
    conf.set(HConstants.ZOOKEEPER_QUORUM, Joiner.on(",").join(uri.getZookeeperQuorumOrdered()));
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, uri.getZookeeperClientPort());

    final HBaseAdmin hbaseAdmin = adminFactory.create(conf);
    try {
      if (hbaseAdmin.tableExists(
          KijiManagedHBaseTableName.getSystemTableName(uri.getInstance()).toString())) {
        throw new KijiAlreadyExistsException(String.format(
            "Kiji instance '%s' already exists.", uri), uri);
      }
      LOG.info(String.format("Installing kiji instance '%s'.", uri));
      HBaseSystemTable.install(hbaseAdmin, uri, conf, properties, tableFactory);
      HBaseMetaTable.install(hbaseAdmin, uri);
      HBaseSchemaTable.install(hbaseAdmin, uri, conf, tableFactory, lockFactory);
      // Grant the current user all privileges on the instance just created, if security is enabled.
      final Kiji kiji = Kiji.Factory.open(uri, conf);
      assert(null != kiji);
      try {
        if (kiji.isSecurityEnabled()) {
          KijiSecurityManager.Installer.installInstanceCreator(uri, conf, tableFactory);
        }
      } finally {
        kiji.release();
      }
    } finally {
      ResourceUtils.closeOrLog(hbaseAdmin);
    }
    LOG.info(String.format("Installed kiji instance '%s'.", uri));
  }

  /**
   * Removes a kiji instance from the HBase cluster including any user tables.
   *
   * @param uri URI of the Kiji instance to install.
   * @param hbaseFactory Factory for HBase instances.
   * @param conf Hadoop configuration.
   * @throws java.io.IOException on I/O error.
   * @throws org.kiji.schema.KijiInvalidNameException if the instance name is invalid.
   * @throws org.kiji.schema.KijiNotInstalledException if the specified instance does not exist.
   */
  public void uninstall(KijiURI uri, HBaseFactory hbaseFactory, Configuration conf)
      throws IOException {
    if (uri.getInstance() == null) {
      throw new KijiInvalidNameException(String.format(
          "Kiji URI '%s' does not specify a Kiji instance name", uri));
    }
    final HBaseAdminFactory adminFactory = hbaseFactory.getHBaseAdminFactory(uri);

    // TODO: Factor this in HBaseKiji
    conf.set(HConstants.ZOOKEEPER_QUORUM, Joiner.on(",").join(uri.getZookeeperQuorumOrdered()));
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, uri.getZookeeperClientPort());

    LOG.info(String.format("Removing the kiji instance '%s'.", uri.getInstance()));

    final Kiji kiji = Kiji.Factory.open(uri, conf);
    try {
      // If security is enabled, make sure the user has GRANT access on the instance
      // before uninstalling.
      if (kiji.isSecurityEnabled()) {
        KijiSecurityManager securityManager = kiji.getSecurityManager();
        try {
          securityManager.checkCurrentGrantAccess();
        } finally {
          securityManager.close();
        }
      }

      for (String tableName : kiji.getTableNames()) {
        LOG.debug("Deleting kiji table " + tableName + "...");
        kiji.deleteTable(tableName);
      }

      // Delete the user tables:
      final HBaseAdmin hbaseAdmin = adminFactory.create(conf);
      try {

        // Delete the system tables:
        HBaseSystemTable.uninstall(hbaseAdmin, uri);
        HBaseMetaTable.uninstall(hbaseAdmin, uri);
        HBaseSchemaTable.uninstall(hbaseAdmin, uri);

      } finally {
        ResourceUtils.closeOrLog(hbaseAdmin);
      }

      // Delete ZNodes from ZooKeeper
      final CuratorFramework zkClient =
          ZooKeeperUtils.getZooKeeperClient(uri);
      try {
        zkClient
            .delete()
            .deletingChildrenIfNeeded()
            .forPath(ZooKeeperUtils.getInstanceDir(uri).getPath());
      } catch (Exception e) {
        ZooKeeperUtils.wrapAndRethrow(e);
      } finally {
        zkClient.close();
      }
    } finally {
      kiji.release();
    }
    LOG.info(String.format("Removed kiji instance '%s'.", uri.getInstance()));
  }

  /**
   * Gets an instance of a KijiInstaller.
   *
   * @return An instance of a KijiInstaller.
   */
  public static HBaseKijiInstaller get() {
    return new HBaseKijiInstaller();
  }

  /** {@inheritDoc} */
  @Override
  public int getPriority(Map<String, String> runtimeHints) {
    Preconditions.checkArgument(runtimeHints.containsKey(Kiji.KIJI_TYPE_KEY));
    if (runtimeHints.get(Kiji.KIJI_TYPE_KEY).equals(KijiURI.TYPE_HBASE)) {
      return Priority.NORMAL;
    } else {
      return Priority.DISABLED;
    }
  }
}
