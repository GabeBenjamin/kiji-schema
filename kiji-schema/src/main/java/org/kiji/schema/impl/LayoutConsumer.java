/**
 * (c) Copyright 2013 WibiData, Inc.
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
package org.kiji.schema.impl;

import java.io.IOException;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * A callback interface which classes can override in order to be registered to receive
 * notifications through calls to {@link #update(KijiTableLayout)} that a new
 * {@link KijiTableLayout} is available for the table.
 */
@ApiAudience.Private
public interface LayoutConsumer {

  /**
   * Called when the table layout changes.
   *
   * @param layout The most recent layout of the Kiji table.
   * @throws IOException in case of an error updating.
   */
  void update(KijiTableLayout layout) throws IOException;
}
