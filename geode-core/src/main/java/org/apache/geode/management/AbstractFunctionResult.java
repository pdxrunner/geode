/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;
import org.apache.geode.management.cli.CliFunctionResult;

@Experimental
public class AbstractFunctionResult
    implements FunctionResult {
  protected String memberIdOrName;
  protected Serializable[] serializables = new String[0];
  protected Object resultObject;
  protected byte[] byteData = new byte[0];
  protected FunctionResultStatusState state;

  @Override
  public String getMemberIdOrName() {
    return this.memberIdOrName;
  }

  @Override
  @Deprecated
  public String getMessage() {
    if (this.serializables.length == 0 || !(this.serializables[0] instanceof String)) {
      return null;
    }

    return (String) this.serializables[0];
  }

  @Override
  public String getStatus() {
    return state.name();
  }

  @Override
  public String getStatusMessage() {
    String message = getMessage();

    if (isSuccessful()) {
      return message;
    }

    String errorMessage = "";
    if (message != null
        && (resultObject == null || !((Throwable) resultObject).getMessage().contains(message))) {
      errorMessage = message;
    }

    if (resultObject != null) {
      errorMessage = errorMessage.trim() + " " + ((Throwable) resultObject).getClass().getName()
          + ": " + ((Throwable) resultObject).getMessage();
    }

    return errorMessage;
  }

  @Override
  @Deprecated
  public Serializable[] getSerializables() {
    return this.serializables;
  }

  @Override
  @Deprecated
  public Throwable getThrowable() {
    if (isSuccessful()) {
      return null;
    }
    return ((Throwable) resultObject);
  }

  @Override
  public Object getResultObject() {
    return resultObject;
  }

  @Override
  public int getDSFID() {
    return DataSerializableFixedID.CLI_FUNCTION_RESULT;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.memberIdOrName, out);
    DataSerializer.writePrimitiveBoolean(this.isSuccessful(), out);
    // DataSerializer.writeObject(this.xmlEntity, out);
    DataSerializer.writeObjectArray(this.serializables, out);
    DataSerializer.writeObject(this.resultObject, out);
    DataSerializer.writeByteArray(this.byteData, out);
    // toDataPre_GEODE_1_6_0_0(out);
    DataSerializer.writeEnum(this.state, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.memberIdOrName = DataSerializer.readString(in);
    this.state = DataSerializer.readPrimitiveBoolean(in) ? FunctionResultStatusState.OK
        : FunctionResultStatusState.ERROR;
    // this.xmlEntity = DataSerializer.readObject(in);
    this.serializables = (Serializable[]) DataSerializer.readObjectArray(in);
    this.resultObject = DataSerializer.readObject(in);
    this.byteData = DataSerializer.readByteArray(in);
    // fromDataPre_GEODE_1_6_0_0(in);
    this.state = DataSerializer.readEnum(FunctionResultStatusState.class, in);
  }

  @Override
  public boolean isSuccessful() {
    return this.state == FunctionResultStatusState.OK;
  }

  @Override
  @Deprecated
  public byte[] getByteData() {
    return this.byteData;
  }

  @Override
  public int compareTo(CliFunctionResult o) {
    if (this.memberIdOrName == null && o.memberIdOrName == null) {
      return 0;
    }
    if (this.memberIdOrName == null && o.memberIdOrName != null) {
      return -1;
    }
    if (this.memberIdOrName != null && o.memberIdOrName == null) {
      return 1;
    }
    return getMemberIdOrName().compareTo(o.memberIdOrName);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((this.memberIdOrName == null) ? 0 : this.memberIdOrName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    CliFunctionResult other = (CliFunctionResult) obj;
    if (this.memberIdOrName == null) {
      if (other.memberIdOrName != null)
        return false;
    } else if (!this.memberIdOrName.equals(other.memberIdOrName))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "CliFunctionResult [memberId=" + this.memberIdOrName + ", successful="
        + this.isSuccessful() + ", serializables="
        + Arrays.toString(this.serializables) + ", throwable=" + this.resultObject + ", byteData="
        + Arrays.toString(this.byteData) + "]";
  }

  @Override
  public Version[] getSerializationVersions() {
    return new Version[] {Version.GFE_80};
  }
}
