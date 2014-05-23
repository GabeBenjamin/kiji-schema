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
package org.kiji.schema.cassandra;

import java.nio.ByteBuffer;

import com.google.common.base.Objects;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.impl.cassandra.CassandraByteUtil;

/**
 * A Cassandra column name.
 */
@ApiAudience.Public
@ApiStability.Evolving
public final class CassandraColumnName {

  /** The Locality Group name. */
  private final String mLocalityGroup;

  /** The Cassandra column family. */
  private final byte[] mFamily;

  /** The Cassandra column qualifier. */
  private final byte[] mQualifier;

  /**
   * Creates a new {@link CassandraColumnName} instance.
   *
   * @param localityGroup Cassandra locality group, not null.
   * @param family Cassandra column family, not null.
   * @param qualifier Cassandra column qualifier.
   */
  public CassandraColumnName(String localityGroup, byte[] family, byte[] qualifier) {
    mLocalityGroup = localityGroup;
    mFamily = family;
    mQualifier = qualifier;
  }

  /**
   * Creates a new {@link CassandraColumnName} instance.
   *
   * @param localityGroup Cassandra locality group, not null.
   * @param family Cassandra column family, not null.
   * @param qualifier Cassandra column qualifier.
   */
  public CassandraColumnName(String localityGroup, ByteBuffer family, ByteBuffer qualifier) {
    mLocalityGroup = localityGroup;
    mFamily = CassandraByteUtil.byteBuffertoBytes(family);
    mQualifier = qualifier == null ? null : CassandraByteUtil.byteBuffertoBytes(qualifier);
  }

  /**
   * Gets the Cassandra locality group.
   *
   * @return the locality group.
   */
  public String getLocalityGroup() {
    return mLocalityGroup;
  }

  /**
   * Gets the Cassandra column family.  Do *not* mutate the returned byte array.
   *
   * @return the family.
   */
  public byte[] getFamily() {
    return mFamily;
  }

  /**
   * Gets the Cassandra column qualifier.  Do *not* mutate the returned byte array.
   *
   * @return The qualifier.
   */
  public byte[] getQualifier() {
    return mQualifier;
  }

  /**
   * Gets the Cassandra column family.  Do *not* mutate the bytes in the returned byte buffer.
   *
   * @return the family.
   */
  public ByteBuffer getFamilyBuffer() {
    return ByteBuffer.wrap(mFamily);
  }

  /**
   * Gets the Cassandra column qualifier.  Do *not* mutate the bytes in the returned byte buffer.
   *
   * @return The qualifier.
   */
  public ByteBuffer getQualifierBuffer() {
    return mQualifier == null ? null : ByteBuffer.wrap(mQualifier);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects
        .toStringHelper(CassandraColumnName.class)
        .add("locality group", mLocalityGroup)
        .add("family", Bytes.toStringBinary(mFamily))
        .add("qualifier", Bytes.toStringBinary(mQualifier))
        .toString();
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(mLocalityGroup)
        .append(mFamily)
        .append(mQualifier)
        .toHashCode();
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof CassandraColumnName)) {
      return false;
    }
    final CassandraColumnName other = (CassandraColumnName) obj;
    return new EqualsBuilder()
        .append(mLocalityGroup, other.mLocalityGroup)
        .append(mFamily, other.mFamily)
        .append(mQualifier, other.mQualifier)
        .isEquals();
  }
}
