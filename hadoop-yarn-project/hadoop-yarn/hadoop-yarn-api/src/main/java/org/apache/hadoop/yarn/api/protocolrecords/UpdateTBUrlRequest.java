package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by wangqiang on 2017/8/16.
 */
public abstract class UpdateTBUrlRequest {

  public static UpdateTBUrlRequest newInstance(ApplicationId applicationId, String tensorboardUrl) {
    UpdateTBUrlRequest request = Records.newRecord(UpdateTBUrlRequest.class);
    request.setApplicationId(applicationId);
    request.setTensorboardUrl(tensorboardUrl);
    return request;
  }

  public abstract void setApplicationId(ApplicationId applicationId);


  public abstract ApplicationId getApplicationId();

  public abstract void setTensorboardUrl(String tensorboardUrl);

  public abstract String getTensorboardUrl();
}
