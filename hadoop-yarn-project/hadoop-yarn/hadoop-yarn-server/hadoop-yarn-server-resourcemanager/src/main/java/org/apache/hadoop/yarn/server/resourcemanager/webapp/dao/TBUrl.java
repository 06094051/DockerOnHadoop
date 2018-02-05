package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Created by wangqiang on 2017/8/16.
 */

@XmlRootElement(name = "tburl")
@XmlAccessorType(XmlAccessType.FIELD)
public class TBUrl {
  String tensorboardUrl;

  public TBUrl() {
  }

  public TBUrl(String tensorboardUrl) {
    this.tensorboardUrl = tensorboardUrl;
  }

  public String getTensorboardUrl() {
    return tensorboardUrl;
  }

  public void setTensorboardUrl(String tensorboardUrl) {
    this.tensorboardUrl = tensorboardUrl;
  }
}
