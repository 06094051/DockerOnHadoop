package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateTBUrlResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.UpdateTBUrlResponseProto;

/**
 * Created by wangqiang on 2017/8/16.
 */
public class UpdateTBUrlResponsePBImpl extends UpdateTBUrlResponse {

  UpdateTBUrlResponseProto proto = UpdateTBUrlResponseProto.getDefaultInstance();
  UpdateTBUrlResponseProto.Builder builder = null;
  boolean viaProto = false;

  private ApplicationId applicationId = null;

  public UpdateTBUrlResponsePBImpl() {
    builder = UpdateTBUrlResponseProto.newBuilder();
  }

  public UpdateTBUrlResponsePBImpl(UpdateTBUrlResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public UpdateTBUrlResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  private void mergeLocalToBuilder() {
    if (this.applicationId != null) {
      builder.setApplicationId(convertToProtoFormat(this.applicationId));
    }
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = UpdateTBUrlResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public void setTensorboardUrl(String tensorboardUrl) {
    maybeInitBuilder();
    builder.setTensorboardUrl(tensorboardUrl);
  }

  @Override
  public String getTensorboardUrl() {
    YarnServiceProtos.UpdateTBUrlResponseProtoOrBuilder p = viaProto ? proto : builder;
    return p.getTensorboardUrl();
  }

  @Override
  public ApplicationId getApplicationId() {
    YarnServiceProtos.UpdateTBUrlResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.applicationId != null) {
      return this.applicationId;
    }
    if (!p.hasApplicationId()) {
      return null;
    }
    this.applicationId = convertFromProtoFormat(p.getApplicationId());
    return this.applicationId;
  }

  @Override
  public void setApplicationId(ApplicationId applicationId) {
    maybeInitBuilder();
    if (applicationId == null)
      builder.clearApplicationId();
    this.applicationId = applicationId;
  }

  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl) t).getProto();
  }
}


