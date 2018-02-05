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

package org.apache.hadoop.yarn.api.impl.pb.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationClientProtocolPB;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.*;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.UpdateTBUrlRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationAttemptReportRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationAttemptsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationReportRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterMetricsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodeLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerReportRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainersRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetNewApplicationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueInfoRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueUserAclsInfoRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.KillApplicationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.MoveApplicationAcrossQueuesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationDeleteRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationSubmissionRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationUpdateRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SubmitApplicationRequestProto;

import com.google.protobuf.ServiceException;

@Private
public class ApplicationClientProtocolPBClientImpl implements ApplicationClientProtocol,
    Closeable {

  private ApplicationClientProtocolPB proxy;


  @Override
  public UpdateTBUrlResponse updateApplicationTBUrl(UpdateTBUrlRequest request) throws YarnException, IOException {
    UpdateTBUrlRequestProto requestProto =
        ((UpdateTBUrlRequestPBImpl) request).getProto();
    try {
      return new UpdateTBUrlResponsePBImpl(proxy.updateApplicationTBUrl(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  public ApplicationClientProtocolPBClientImpl(long clientVersion,
      InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, ApplicationClientProtocolPB.class,
      ProtobufRpcEngine.class);
    proxy = RPC.getProxy(ApplicationClientProtocolPB.class, clientVersion, addr, conf);
  }

  @Override
  public void close() {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
  }

  @Override
  public KillApplicationResponse forceKillApplication(
      KillApplicationRequest request) throws YarnException, IOException {
    KillApplicationRequestProto requestProto =
        ((KillApplicationRequestPBImpl) request).getProto();
    try {
      return new KillApplicationResponsePBImpl(proxy.forceKillApplication(null,
        requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetApplicationReportResponse getApplicationReport(
      GetApplicationReportRequest request) throws YarnException,
      IOException {
    GetApplicationReportRequestProto requestProto =
        ((GetApplicationReportRequestPBImpl) request).getProto();
    try {
      return new GetApplicationReportResponsePBImpl(proxy.getApplicationReport(
        null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetClusterMetricsResponse getClusterMetrics(
      GetClusterMetricsRequest request) throws YarnException,
      IOException {
    GetClusterMetricsRequestProto requestProto =
        ((GetClusterMetricsRequestPBImpl) request).getProto();
    try {
      return new GetClusterMetricsResponsePBImpl(proxy.getClusterMetrics(null,
        requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetNewApplicationResponse getNewApplication(
      GetNewApplicationRequest request) throws YarnException,
      IOException {
    GetNewApplicationRequestProto requestProto =
        ((GetNewApplicationRequestPBImpl) request).getProto();
    try {
      return new GetNewApplicationResponsePBImpl(proxy.getNewApplication(null,
        requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) throws YarnException,
      IOException {
    SubmitApplicationRequestProto requestProto =
        ((SubmitApplicationRequestPBImpl) request).getProto();
    try {
      return new SubmitApplicationResponsePBImpl(proxy.submitApplication(null,
        requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetApplicationsResponse getApplications(
      GetApplicationsRequest request) throws YarnException,
      IOException {
    GetApplicationsRequestProto requestProto =
        ((GetApplicationsRequestPBImpl) request).getProto();
    try {
      return new GetApplicationsResponsePBImpl(proxy.getApplications(
        null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetClusterNodesResponse
      getClusterNodes(GetClusterNodesRequest request)
          throws YarnException, IOException {
    GetClusterNodesRequestProto requestProto =
        ((GetClusterNodesRequestPBImpl) request).getProto();
    try {
      return new GetClusterNodesResponsePBImpl(proxy.getClusterNodes(null,
        requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
      throws YarnException, IOException {
    GetQueueInfoRequestProto requestProto =
        ((GetQueueInfoRequestPBImpl) request).getProto();
    try {
      return new GetQueueInfoResponsePBImpl(proxy.getQueueInfo(null,
        requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetQueueUserAclsInfoResponse getQueueUserAcls(
      GetQueueUserAclsInfoRequest request) throws YarnException,
      IOException {
    GetQueueUserAclsInfoRequestProto requestProto =
        ((GetQueueUserAclsInfoRequestPBImpl) request).getProto();
    try {
      return new GetQueueUserAclsInfoResponsePBImpl(proxy.getQueueUserAcls(
        null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) throws YarnException,
      IOException {
    GetDelegationTokenRequestProto requestProto =
        ((GetDelegationTokenRequestPBImpl) request).getProto();
    try {
      return new GetDelegationTokenResponsePBImpl(proxy.getDelegationToken(
        null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public RenewDelegationTokenResponse renewDelegationToken(
      RenewDelegationTokenRequest request) throws YarnException,
      IOException {
    RenewDelegationTokenRequestProto requestProto =
        ((RenewDelegationTokenRequestPBImpl) request).getProto();
    try {
      return new RenewDelegationTokenResponsePBImpl(proxy.renewDelegationToken(
          null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public CancelDelegationTokenResponse cancelDelegationToken(
      CancelDelegationTokenRequest request) throws YarnException,
      IOException {
    CancelDelegationTokenRequestProto requestProto =
        ((CancelDelegationTokenRequestPBImpl) request).getProto();
    try {
      return new CancelDelegationTokenResponsePBImpl(
          proxy.cancelDelegationToken(null, requestProto));

    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public MoveApplicationAcrossQueuesResponse moveApplicationAcrossQueues(
      MoveApplicationAcrossQueuesRequest request) throws YarnException,
      IOException {
    MoveApplicationAcrossQueuesRequestProto requestProto =
        ((MoveApplicationAcrossQueuesRequestPBImpl) request).getProto();
    try {
      return new MoveApplicationAcrossQueuesResponsePBImpl(
          proxy.moveApplicationAcrossQueues(null, requestProto));

    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetApplicationAttemptReportResponse getApplicationAttemptReport(
      GetApplicationAttemptReportRequest request) throws YarnException,
      IOException {
    GetApplicationAttemptReportRequestProto requestProto =
        ((GetApplicationAttemptReportRequestPBImpl) request).getProto();
    try {
      return new GetApplicationAttemptReportResponsePBImpl(
        proxy.getApplicationAttemptReport(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetApplicationAttemptsResponse getApplicationAttempts(
      GetApplicationAttemptsRequest request) throws YarnException, IOException {
    GetApplicationAttemptsRequestProto requestProto =
        ((GetApplicationAttemptsRequestPBImpl) request).getProto();
    try {
      return new GetApplicationAttemptsResponsePBImpl(
        proxy.getApplicationAttempts(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetContainerReportResponse getContainerReport(
      GetContainerReportRequest request) throws YarnException, IOException {
    GetContainerReportRequestProto requestProto =
        ((GetContainerReportRequestPBImpl) request).getProto();
    try {
      return new GetContainerReportResponsePBImpl(proxy.getContainerReport(
        null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetContainersResponse getContainers(GetContainersRequest request)
      throws YarnException, IOException {
    GetContainersRequestProto requestProto =
        ((GetContainersRequestPBImpl) request).getProto();
    try {
      return new GetContainersResponsePBImpl(proxy.getContainers(null,
        requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public ReservationSubmissionResponse submitReservation(ReservationSubmissionRequest request)
      throws YarnException, IOException {
    ReservationSubmissionRequestProto requestProto =
        ((ReservationSubmissionRequestPBImpl) request).getProto();
    try {
      return new ReservationSubmissionResponsePBImpl(proxy.submitReservation(null,
          requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public ReservationUpdateResponse updateReservation(ReservationUpdateRequest request)
      throws YarnException, IOException {
    ReservationUpdateRequestProto requestProto =
        ((ReservationUpdateRequestPBImpl) request).getProto();
    try {
      return new ReservationUpdateResponsePBImpl(proxy.updateReservation(null,
          requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public ReservationDeleteResponse deleteReservation(ReservationDeleteRequest request)
      throws YarnException, IOException {
    ReservationDeleteRequestProto requestProto =
        ((ReservationDeleteRequestPBImpl) request).getProto();
    try {
      return new ReservationDeleteResponsePBImpl(proxy.deleteReservation(null,
          requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }


  @Override
  public GetNodesToLabelsResponse getNodeToLabels(
      GetNodesToLabelsRequest request)
      throws YarnException, IOException {
    YarnServiceProtos.GetNodesToLabelsRequestProto
        requestProto =
        ((GetNodesToLabelsRequestPBImpl) request).getProto();
    try {
      return new GetNodesToLabelsResponsePBImpl(proxy.getNodeToLabels(
          null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetLabelsToNodesResponse getLabelsToNodes(
      GetLabelsToNodesRequest request)
      throws YarnException, IOException {
    YarnServiceProtos.GetLabelsToNodesRequestProto requestProto =
        ((GetLabelsToNodesRequestPBImpl) request).getProto();
    try {
      return new GetLabelsToNodesResponsePBImpl(proxy.getLabelsToNodes(
          null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetClusterNodeLabelsResponse getClusterNodeLabels(
      GetClusterNodeLabelsRequest request) throws YarnException, IOException {
      GetClusterNodeLabelsRequestProto
        requestProto =
        ((GetClusterNodeLabelsRequestPBImpl) request).getProto();
    try {
      return new GetClusterNodeLabelsResponsePBImpl(proxy.getClusterNodeLabels(
          null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }
}
