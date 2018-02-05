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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.commons.logging.Log;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSSchedulerNode;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.Collection;
import java.util.Map;

public class SchedulerAppUtils {

  public static  boolean isBlacklisted(SchedulerApplicationAttempt application,
      SchedulerNode node, Log LOG) {
    if (application.isBlacklisted(node.getNodeName())) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping 'host' " + node.getNodeName() +
            " for " + application.getApplicationId() +
            " since it has been blacklisted");
      }
      return true;
    }

    if (application.isBlacklisted(node.getRackName())) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping 'rack' " + node.getRackName() +
            " for " + application.getApplicationId() +
            " since it has been blacklisted");
      }
      return true;
    }

    return false;
  }

  // 判断 app 能否在 Node 中分配
  public static boolean fitsAssignContainer(FSAppAttempt sched, FSSchedulerNode node){
    Resource available =  node.getAvailableResource();

    // 节点能否分配资源
    if(!Resources.fitsIn(sched.getScheduler().getMinimumResourceCapability(), available))
      return false;

    Collection<Priority> ps = sched.appSchedulingInfo.getPriorities();
    for(Priority priority : ps){
      int num = sched.getTotalRequiredResources(priority);
      if(num <= 0 ){
        continue;
      }
      Map<String, ResourceRequest> resourceRequests = sched.appSchedulingInfo.getResourceRequests(priority);

      for(String name :resourceRequests.keySet()){
        Resource capability = resourceRequests.get(name).getCapability();
        // 防止一台机器启动的app master container太多
        if(node.getNumContainers() > sched.getScheduler().getMaxContainerPerNode() && capability.getGpuCores() == 0){
          return false;
        }
        if(resourceRequests.get(name).getNumContainers() > 0 && Resources.fitsIn(capability, available)){
          Resource left = Resources.subtract(available, resourceRequests.get(name).getCapability());

          if(left.getGpuCores() == 0)
            return true;
          // 符合预留给的可以分配资源
          if(left.getGpuCores() > 0 &&
              left.getMemory() >= (left.getGpuCores() * sched.getScheduler().getReserveMemoryPerGCore()) +sched.getScheduler().getReserveMemoryBaseGCore() &&
              left.getVirtualCores() >= (left.getGpuCores() * sched.getScheduler().getReservevCoresPerGCore() + sched.getScheduler().getReservevCoresBaseGCore())){
            return true;
          }
        }
      }
      return false;
    }
    return true;
  }
}
