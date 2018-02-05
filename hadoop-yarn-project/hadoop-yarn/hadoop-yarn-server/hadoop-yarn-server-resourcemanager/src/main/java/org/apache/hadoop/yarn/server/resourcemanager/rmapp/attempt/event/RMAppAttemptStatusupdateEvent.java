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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;

public class RMAppAttemptStatusupdateEvent extends RMAppAttemptEvent {

  private final float progress;
  private final String tensorboardUrl;
  private final boolean updateTensorboardUrl;

  public RMAppAttemptStatusupdateEvent(ApplicationAttemptId appAttemptId,
      float progress) {
    super(appAttemptId, RMAppAttemptEventType.STATUS_UPDATE);
    this.progress = progress;
    this.tensorboardUrl = null;
    this.updateTensorboardUrl = false;
  }

  public RMAppAttemptStatusupdateEvent(ApplicationAttemptId appAttemptId, String tensorboardUrl) {
    super(appAttemptId, RMAppAttemptEventType.STATUS_UPDATE);
    this.progress = 10;
    this.tensorboardUrl = tensorboardUrl;
    this.updateTensorboardUrl = true;
  }


  public float getProgress() {
    return this.progress;
  }

  public String getTensorboardUrl(){return this.tensorboardUrl;}
  public boolean isUpdateTensorboardUrl(){return this.updateTensorboardUrl;}

}
