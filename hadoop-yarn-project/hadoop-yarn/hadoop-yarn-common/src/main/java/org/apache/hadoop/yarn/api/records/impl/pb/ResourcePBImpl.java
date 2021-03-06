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

package org.apache.hadoop.yarn.api.records.impl.pb;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProtoOrBuilder;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Private
@Unstable
public class ResourcePBImpl extends Resource {

  private Set<Integer> gpusInfo = null;
  ResourceProto proto = ResourceProto.getDefaultInstance();
  ResourceProto.Builder builder = null;
  boolean viaProto = false;
  
  public ResourcePBImpl() {
    builder = ResourceProto.newBuilder();
    ts = System.currentTimeMillis();
  }

  public ResourcePBImpl(ResourceProto proto) {
    this.proto = proto;
    viaProto = true;
    ts = System.currentTimeMillis();
  }
  
  public ResourceProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }


  private void mergeLocalToBuilder() {
    if (this.gpusInfo != null) {
      builder.clearGpusInfo();
      builder.addAllGpusInfo(this.getGpusInfo());
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ResourceProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public int getMemory() {
    ResourceProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getMemory());
  }

  @Override
  public void setMemory(int memory) {
    maybeInitBuilder();
    builder.setMemory((memory));
  }

  @Override
  public int getVirtualCores() {
    ResourceProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getVirtualCores());
  }

  @Override
  public void setVirtualCores(int vCores) {
    maybeInitBuilder();
    builder.setVirtualCores((vCores));
  }

  @Override
  public int getGpuCores() {
    ResourceProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getGpuCores());
  }

  @Override
  public void setGpuCores(int gCores) {
    maybeInitBuilder();
    builder.setGpuCores((gCores));
  }

  @Override
  public Set<Integer> getGpusInfo() {
    initGpusInfo();
    return this.gpusInfo;
  }

  private void initGpusInfo() {
    if(this.ts < 0){
      ts = System.currentTimeMillis();
    }
    if (this.gpusInfo != null) {
      return;
    }
    ResourceProtoOrBuilder p = viaProto ? proto : builder;
    this.gpusInfo = new HashSet<Integer>();
    this.gpusInfo.addAll(p.getGpusInfoList());
  }


  @Override
  public void setGpusInfo(Set<Integer> gpusInfo) {
    if(ts < 0){
      ts = System.currentTimeMillis();
    }
    maybeInitBuilder();
    builder.clearGpusInfo();
    this.gpusInfo = gpusInfo;
  }

  @Override
  public int compareTo(Resource other) {
    int diff = this.getMemory() - other.getMemory();
    if (diff == 0) {
      diff = this.getVirtualCores() - other.getVirtualCores();
      if (diff == 0) {
        diff = this.getGpuCores() - other.getGpuCores();
      }
    }
    return diff;
  }
}  
