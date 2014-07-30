/**
 * (c) Copyright 2014 WibiData, Inc.
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

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;

/**
 * Interface for modifying a Kiji table.
 *
 * <p>
 *   Wraps methods from KijiPutter, KijiIncrementer, and KijiDeleter.
 *   To get a KijiTableWriter, use {@link KijiTable#openTableWriter()}
 *   or {@link KijiTable#getWriterFactory()}.
 * </p>
 *
 * <p>
 *   Unless otherwise specified, writers are not thread-safe and must be synchronized externally.
 * </p>
 */
@ApiAudience.Public
@ApiStability.Evolving
@Inheritance.Sealed
public interface AsyncKijiTableWriter extends AsyncKijiPutter, AsyncKijiIncrementer, KijiDeleter {
}