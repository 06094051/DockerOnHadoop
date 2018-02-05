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

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.collect.ImmutableSet;


/**
 * Represents a YARN Cluster Node from the viewpoint of the scheduler.
 */
@Private
@Unstable
public abstract class SchedulerNode {

  private static final Log LOG = LogFactory.getLog(SchedulerNode.class);

  private Resource availableResource = Resource.newInstance(0, 0, 0);
  private Resource usedResource = Resource.newInstance(0, 0, 0);
  private Resource totalResourceCapability;
  private RMContainer reservedContainer;
  private volatile int numContainers;


  /* set of containers that are allocated containers */
  private final Map<ContainerId, RMContainer> launchedContainers =
      new HashMap<ContainerId, RMContainer>();

  private final RMNode rmNode;
  private final String nodeName;
  
  private volatile Set<String> labels = null;

  //TODO 需要强制物理机的gpu序号是从0 开始，如果不是，代码需要重新开发
  // 当前节点gpu信息，暂时用 0-n 表示这 n + 1 块GPU
  private Set<Integer> totalGPUS = new HashSet<Integer>();

  public SchedulerNode(RMNode node, boolean usePortForNodeName,
      Set<String> labels) {
    this.rmNode = node;
    this.availableResource = Resources.clone(node.getTotalCapability());
    this.totalResourceCapability = Resources.clone(node.getTotalCapability());
    if (usePortForNodeName) {
      nodeName = rmNode.getHostName() + ":" + node.getNodeID().getPort();
    } else {
      nodeName = rmNode.getHostName();
    }
    this.labels = ImmutableSet.copyOf(labels);

    for(int i = 0; i< totalResourceCapability.getGpuCores(); i++){
      totalGPUS.add(i);
    }
  }

  public SchedulerNode(RMNode node, boolean usePortForNodeName) {
    this(node, usePortForNodeName, CommonNodeLabelsManager.EMPTY_STRING_SET);
  }

  public RMNode getRMNode() {
    return this.rmNode;
  }

  /**
   * Set total resources on the node.
   * @param resource total resources on the node.
   */
  public synchronized void setTotalResource(Resource resource){
    this.totalResourceCapability = resource;
    this.availableResource = Resources.subtract(totalResourceCapability,
      this.usedResource);
    for(int i = 0;i< totalResourceCapability.getGpuCores();i++){
      totalGPUS.add(i);
    }
  }
  
  /**
   * Get the ID of the node which contains both its hostname and port.
   * 
   * @return the ID of the node
   */
  public NodeId getNodeID() {
    return this.rmNode.getNodeID();
  }

  public Set<Integer> getTotalGPUS() {
    return totalGPUS;
  }


  public String getHttpAddress() {
    return this.rmNode.getHttpAddress();
  }

  /**
   * Get the name of the node for scheduling matching decisions.
   * <p>
   * Typically this is the 'hostname' reported by the node, but it could be
   * configured to be 'hostname:port' reported by the node via the
   * {@link YarnConfiguration#RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME} constant.
   * The main usecase of this is Yarn minicluster to be able to differentiate
   * node manager instances by their port number.
   * 
   * @return name of the node for scheduling matching decisions.
   */
  public String getNodeName() {
    return nodeName;
  }

  /**
   * Get rackname.
   * 
   * @return rackname
   */
  public String getRackName() {
    return this.rmNode.getRackName();
  }

  /**
   * The Scheduler has allocated containers on this node to the given
   * application.
   * 
   * @param rmContainer
   *          allocated container
   */
  public synchronized void allocateContainer(RMContainer rmContainer) {
    Container container = rmContainer.getContainer();
    deductAvailableResource(container.getResource());
    ++numContainers;
    launchedContainers.put(container.getId(), rmContainer);
    allocateGPUINFO(rmContainer);
    LOG.info("Assigned container " + container.getId() + " of capacity "
        + container.getResource() + " on host " + rmNode.getNodeAddress()
        + ", which has " + numContainers + " containers, "
        + getUsedResource() + " used and " + getAvailableResource()
        + " available after allocation");
  }

  /**
   * 如果出现  need check code ............ 日志，需要跟踪场景，正常情况（或者没有考虑的场景）是没有这种情况发生
   * @param rmContainer
   */
  private void allocateGPUINFO(RMContainer rmContainer){
    Container container = rmContainer.getContainer();
    int numGPUS = container.getResource().getGpuCores();
    //TODO check下申请的gpu 是不是比当前节点最大的gpu个数大，如果大则打印日志，需要检查这种异常场景来源，原则上是不可能有这种情况
    if(numGPUS > totalResourceCapability.getGpuCores()){
      LOG.warn(container.getId() + " need " + numGPUS + " larger than " + totalResourceCapability.getGpuCores() + ",need check code ............ ");
      return;
    }
    LOG.info(container.getId() + "  need " + numGPUS + " gpu.");
    if(numGPUS == 0){
      LOG.info(container.getId() + "  need 0 gpu, ignore allocate.");
      return;
    }
    // HA recovery 模式 会走这个场景
    if(numGPUS == container.getResource().getGpusInfo().size()){
      LOG.info(container.getId() + " has allocate " + container.getResource().getGpusInfo() +". ignore allocate.");
      return;
    }

    Set<Integer> usedGPUS = new HashSet<>();

    for(ContainerId containerId : launchedContainers.keySet()){
      if(!containerId.equals(container.getId())) {
        LOG.info(getNodeID() + "  has allocated " + launchedContainers.get(containerId).getContainer());
        usedGPUS.addAll(launchedContainers.get(containerId).getContainer().getResource().getGpusInfo());
      }else {
        LOG.info(getNodeID() + "  has allocated " + launchedContainers.get(containerId).getContainer() + " need allocate gpu info.");
      }
    }
    LOG.info(getNodeID() + " has " + totalGPUS + " allocated " + usedGPUS + ". begin allocate gpu info to " + container.getId() + ", need " + numGPUS + " gpu info. ");
    Set<Integer> info = new HashSet<Integer>();
    info.addAll(container.getResource().getGpusInfo());
    //TODO gpu 分配是原子的，需要检查下为什么当前容器的gpu分配了一部分
    if(info.size() != 0){
      LOG.warn(container.getId() + "'s gpu info is not empty, need check code ...............");
      info.clear();
    }
    int startIndex = 0;
    for(Integer i : usedGPUS){
      if(i > startIndex)
        startIndex = i;
    }
    int maxGPU = totalGPUS.size();
    //TODO 需要检查下为什么分配了 totalGPUS 没有的gpu序号
    if(!totalGPUS.containsAll(usedGPUS)){
      LOG.warn(getNodeID() + " gpu allocate error. allocate: " + usedGPUS + ", total: " + totalGPUS + " need check code ...........");
    }
    LOG.info(container.getId() + " begin allocate gpu info resource:" + container.getResource() + " resource hashcode:" + container.getResource().hashCode());
    while(info.size() < numGPUS){
      if(!usedGPUS.contains(startIndex % maxGPU)){
        LOG.info(getNodeID() + " allocate gpu [ " + (startIndex % maxGPU) + " ] to " + container.getId());
        usedGPUS.add(startIndex % maxGPU);
        info.add(startIndex % maxGPU);
      }
      startIndex ++;
      if(usedGPUS.size() >= totalGPUS.size()){
        LOG.info(getNodeID() + " used gpu info " + usedGPUS + " and  total gpu info is "+ totalGPUS );
        break;
      }
    }
    Resource resource  = Resource.newInstance(container.getResource().getMemory(),container.getResource().getVirtualCores(), container.getResource().getGpuCores());
    resource.setGpusInfo(info);
    container.setResource(resource);
    LOG.info(container.getId() + " allocated Resouce:" + resource + ",  Resource hashcode:" + resource.hashCode());
    LOG.info(getNodeID() + " has allocate gpu info: " + info + " to " + container.getId()+ ". be allocated "+ usedGPUS);
  }

  /**
   * Get available resources on the node.
   * 
   * @return available resources on the node
   */
  public synchronized Resource getAvailableResource() {
    return this.availableResource;
  }

  /**
   * Get used resources on the node.
   * 
   * @return used resources on the node
   */
  public synchronized Resource getUsedResource() {
    return this.usedResource;
  }

  /**
   * Get total resources on the node.
   * 
   * @return total resources on the node.
   */
  public synchronized Resource getTotalResource() {
    return this.totalResourceCapability;
  }

  public synchronized boolean isValidContainer(ContainerId containerId) {
    if (launchedContainers.containsKey(containerId)) {
      return true;
    }
    return false;
  }

  private synchronized void updateResource(Container container) {
    addAvailableResource(container.getResource());
    --numContainers;
  }

  /**
   * Release an allocated container on this node.
   * 
   * @param container
   *          container to be released
   */
  public synchronized void releaseContainer(Container container) {
    if (!isValidContainer(container.getId())) {
      LOG.error("Invalid container released " + container);
      return;
    }

    /* remove the containers from the nodemanger */
    if (null != launchedContainers.remove(container.getId())) {
      updateResource(container);
    }

    LOG.info("Released container " + container.getId() + " of capacity "
        + container.getResource() + " on host " + rmNode.getNodeAddress()
        + ", which currently has " + numContainers + " containers, "
        + getUsedResource() + " used and " + getAvailableResource()
        + " available" + ", release resources=" + true);
  }


  public Integer[] getRandomGPU(Integer[] array, int count) {
    Integer[] result = new Integer[count];
    boolean hasTake[] = new boolean[array.length];
    Random random = new Random();
    int m = count;
    if (m > array.length || m < 0)
      return array;
    int n = 0;
    while (true) {
      int temp = random.nextInt(array.length);
      if (!hasTake[temp]) {
        if (n == m)
          break;
        n++;
        result[n - 1] = array[temp];
        hasTake[temp] = true;
      }
    }
    return result;
  }

  private synchronized void addAvailableResource(Resource resource) {
    if (resource == null) {
      LOG.error("Invalid resource addition of null resource for "
          + rmNode.getNodeAddress());
      return;
    }
    Resources.addTo(availableResource, resource);
    Resources.subtractFrom(usedResource, resource);
  }

  private synchronized void deductAvailableResource(Resource resource) {
    if (resource == null) {
      LOG.error("Invalid deduction of null resource for "
          + rmNode.getNodeAddress());
      return;
    }
    Resources.subtractFrom(availableResource, resource);
    Resources.addTo(usedResource, resource);
  }

  /**
   * Reserve container for the attempt on this node.
   */
  public abstract void reserveResource(SchedulerApplicationAttempt attempt,
      Priority priority, RMContainer container);

  /**
   * Unreserve resources on this node.
   */
  public abstract void unreserveResource(SchedulerApplicationAttempt attempt);

  @Override
  public String toString() {
    return "host: " + rmNode.getNodeAddress() + " #containers="
        + getNumContainers() + " available=" + getAvailableResource()
        + " used=" + getUsedResource();
  }

  /**
   * Get number of active containers on the node.
   * 
   * @return number of active containers on the node
   */
  public int getNumContainers() {
    return numContainers;
  }

  public synchronized List<RMContainer> getRunningContainers() {
    return new ArrayList<RMContainer>(launchedContainers.values());
  }

  public synchronized RMContainer getReservedContainer() {
    return reservedContainer;
  }

  protected synchronized void
      setReservedContainer(RMContainer reservedContainer) {
    this.reservedContainer = reservedContainer;
  }

  public synchronized void recoverContainer(RMContainer rmContainer) {
    if (rmContainer.getState().equals(RMContainerState.COMPLETED)) {
      return;
    }
    allocateContainer(rmContainer);
  }
  
  public Set<String> getLabels() {
    return labels;
  }
  
  public void updateLabels(Set<String> labels) {
    this.labels = labels;
  }
}
