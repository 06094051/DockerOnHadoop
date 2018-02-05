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
package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import java.util.List;

@XmlRootElement(name = "appAttempt")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppAttemptInfo {

  protected int id;
  protected long startTime;
  protected String containerId;
  protected String nodeHttpAddress;
  protected String nodeId;
  protected String logsLink;
  protected String blacklistedNodes;


  protected List<ResourceRequest> resourceRequests;
  protected boolean resourcesReady = true;
  protected String tensorboardUrl;

  public AppAttemptInfo() {
  }

  public AppAttemptInfo(ResourceManager rm, RMAppAttempt attempt, String user,
      String schemePrefix) {
    this.startTime = 0;
    this.containerId = "";
    this.nodeHttpAddress = "";
    this.nodeId = "";
    this.logsLink = "";
    this.blacklistedNodes = "";
    if (attempt != null) {
      this.id = attempt.getAppAttemptId().getAttemptId();
      this.startTime = attempt.getStartTime();
      Container masterContainer = attempt.getMasterContainer();
      if (masterContainer != null) {
        this.containerId = masterContainer.getId().toString();
        this.nodeHttpAddress = masterContainer.getNodeHttpAddress();
        this.nodeId = masterContainer.getNodeId().toString();
        this.logsLink = WebAppUtils.getRunningLogURL(schemePrefix
            + masterContainer.getNodeHttpAddress(),
            ConverterUtils.toString(masterContainer.getId()), user);
        if (rm.getResourceScheduler() instanceof AbstractYarnScheduler) {
          AbstractYarnScheduler ayScheduler =
              (AbstractYarnScheduler) rm.getResourceScheduler();
          SchedulerApplicationAttempt sattempt =
              ayScheduler.getApplicationAttempt(attempt.getAppAttemptId());
          if (sattempt != null) {
            blacklistedNodes =
                StringUtils.join(sattempt.getBlacklistedNodes(), ", ");
          }
        }
      }
      this.tensorboardUrl = attempt.getTensorboardUrl();
      this.resourceRequests = ((AbstractYarnScheduler) rm.getResourceScheduler()).
          getPendingResourceRequestsForAttempt(attempt.getAppAttemptId());
      if(attempt.getState() == RMAppAttemptState.RUNNING){
        if (this.resourceRequests != null && this.resourceRequests.size() > 1) {
          for (ResourceRequest rr : this.resourceRequests) {
            if (rr.getNumContainers() > 0)
              this.resourcesReady = false;
          }
        } else {
          this.resourcesReady = false;
        }
      }else{
        this.resourcesReady = false;
      }
    }
  }

  public List<ResourceRequest> getResourceRequests() {
    return resourceRequests;
  }

  public boolean isResourcesReady() {
    return resourcesReady;
  }

  public String getTensorboardUrl() {
    return tensorboardUrl;
  }

  public int getAttemptId() {
    return this.id;
  }

  public long getStartTime() {
    return this.startTime;
  }

  public String getNodeHttpAddress() {
    return this.nodeHttpAddress;
  }

  public String getLogsLink() {
    return this.logsLink;
  }
}
