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

package org.kiji.schema;

import java.io.IOException;
import java.util.Map;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.delegation.PriorityProvider;

/** Installs or uninstalls Kiji instances from an HBase cluster. */
@ApiAudience.Public
@ApiStability.Evolving
public interface KijiInstaller extends PriorityProvider {

  /**
   * Installs the specified Kiji instance.
   *
   * @param uri URI of the Kiji instance to install.
   * @throws IOException on I/O error.
   * @throws KijiInvalidNameException if the Kiji instance name is invalid or already exists.
   */
  void install(KijiURI uri) throws IOException;

  /**
   * Installs the specified Kiji instance.
   *
   * @param uri URI of the Kiji instance to install.
   * @param properties Map of the initial system properties for installation, to be used in addition
   *     to the defaults.
   * @throws IOException on I/O error.
   * @throws KijiInvalidNameException if the Kiji instance name is invalid or already exists.
   */
  void installWithProperties(KijiURI uri, Map<String, String> properties) throws IOException;

  /**
   * Installs the specified Kiji instance.
   *
   * @param uri URI of the Kiji instance to install.
   * @param propertiesFile to use instead of defaults.
   * @throws IOException on I/O error.
   */
  void installWithPropertiesFile(KijiURI uri, String propertiesFile) throws IOException;

  /**
   * Uninstalls the specified Kiji instance.
   *
   * @param uri URI of the Kiji instance to uninstall.
   * @throws IOException on I/O error.
   * @throws KijiInvalidNameException if the instance name is invalid or already exists.
   */
  void uninstall(KijiURI uri) throws IOException;
}
