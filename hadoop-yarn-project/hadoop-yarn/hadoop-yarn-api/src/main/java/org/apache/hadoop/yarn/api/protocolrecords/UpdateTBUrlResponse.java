package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by wangqiang on 2017/8/16.
 */
public abstract class UpdateTBUrlResponse {
  public static UpdateTBUrlResponse newInstance(ApplicationId applicationId, String tensorboardUrl) {
    UpdateTBUrlResponse response = Records.newRecord(UpdateTBUrlResponse.class);
    response.setApplicationId(applicationId);
    response.setTensorboardUrl(tensorboardUrl);
    return response;
  }

  public abstract void setApplicationId(ApplicationId applicationId);


  public abstract ApplicationId getApplicationId();

  public abstract void setTensorboardUrl(String tensorboardUrl);

  public abstract String getTensorboardUrl();

}
