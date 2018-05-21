/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management.internal.cli.functions;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult.StatusState;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

public class GatewaySenderCreateFunction extends CliFunction {

  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = 8746830191680509335L;

  private static final String ID = GatewaySenderCreateFunction.class.getName();

  public static GatewaySenderCreateFunction INSTANCE = new GatewaySenderCreateFunction();


  @Override
  public CliFunctionResult executeFunction(FunctionContext context) {
    // ResultSender<Object> resultSender = context.getResultSender();

    Cache cache = context.getCache();
    String memberNameOrId = context.getMemberName();

    CacheConfig.GatewaySender config = (CacheConfig.GatewaySender) context.getArguments();

    Set<GatewaySender> gatewaySenders = cache.getGatewaySenders();
    GatewaySender createdGatewaySender = createGatewaySender(cache, config);
    // try {
    // GatewaySender createdGatewaySender = createGatewaySender(cache, config);
    // XmlEntity xmlEntity =
    // new XmlEntity(CacheXml.GATEWAY_SENDER, "id", gatewaySenderCreateArgs.getId());
    // resultSender.lastResult(new CliFunctionResult(memberNameOrId, xmlEntity,
    // CliStrings.format(CliStrings.CREATE_GATEWAYSENDER__MSG__GATEWAYSENDER_0_CREATED_ON_1,
    // new Object[] {createdGatewaySender.getId(), memberNameOrId})));
    // } catch (Exception e) {
    // logger.error(e.getMessage(), e);
    // resultSender.lastResult(new CliFunctionResult(memberNameOrId, e, null));
    // }
    return new CliFunctionResult(memberNameOrId, StatusState.OK);
  }

  /**
   * Creates the GatewaySender with given configuration.
   *
   */
  private GatewaySender createGatewaySender(Cache cache, CacheConfig.GatewaySender config) {
    GatewaySenderFactory gateway = cache.createGatewaySenderFactory();

    Boolean isParallel = config.isParallel();
    if (isParallel != null) {
      gateway.setParallel(isParallel);
    }

    Boolean manualStart = config.isManualStart();
    if (manualStart != null) {
      gateway.setManualStart(manualStart);
    }

    Integer maxQueueMemory = Integer.valueOf(config.getMaximumQueueMemory());
    if (maxQueueMemory != null) {
      gateway.setMaximumQueueMemory(maxQueueMemory);
    }

    Integer batchSize = Integer.valueOf(config.getBatchSize());
    if (batchSize != null) {
      gateway.setBatchSize(batchSize);
    }

    Integer batchTimeInterval = Integer.valueOf(config.getBatchTimeInterval());
    if (batchTimeInterval != null) {
      gateway.setBatchTimeInterval(batchTimeInterval);
    }

    Boolean enableBatchConflation = config.isEnableBatchConflation();
    if (enableBatchConflation != null) {
      gateway.setBatchConflationEnabled(enableBatchConflation);
    }

    Integer socketBufferSize = Integer.valueOf(config.getSocketBufferSize());
    if (socketBufferSize != null) {
      gateway.setSocketBufferSize(socketBufferSize);
    }

    String socketReadTimeout = config.getSocketReadTimeout();
    if (StringUtils.isNotEmpty(socketReadTimeout)) {
      gateway.setSocketReadTimeout(Integer.valueOf(socketReadTimeout));
    }

    Integer alertThreshold = Integer.valueOf(config.getAlertThreshold());
    if (alertThreshold != null) {
      gateway.setAlertThreshold(alertThreshold);
    }

    Integer dispatcherThreads = Integer.valueOf(config.getDispatcherThreads());
    if (dispatcherThreads != null && dispatcherThreads > 1) {
      gateway.setDispatcherThreads(dispatcherThreads);

      String orderPolicy = config.getOrderPolicy();
      gateway.setOrderPolicy(OrderPolicy.valueOf(orderPolicy));
    }

    Boolean isPersistenceEnabled = config.isEnablePersistence();
    if (isPersistenceEnabled != null) {
      gateway.setPersistenceEnabled(isPersistenceEnabled);
    }

    String diskStoreName = config.getDiskStoreName();
    if (diskStoreName != null) {
      gateway.setDiskStoreName(diskStoreName);
    }

    Boolean isDiskSynchronous = config.isDiskSynchronous();
    if (isDiskSynchronous != null) {
      gateway.setDiskSynchronous(isDiskSynchronous);
    }

    List<DeclarableType> gatewayEventFilters = config.getGatewayEventFilter();
    if (gatewayEventFilters != null) {
      for (DeclarableType gatewayEventFilter : gatewayEventFilters) {
        Class gatewayEventFilterKlass = forName(gatewayEventFilter.getClassName(),
            CliStrings.CREATE_GATEWAYSENDER__GATEWAYEVENTFILTER);
        gateway.addGatewayEventFilter((GatewayEventFilter) newInstance(gatewayEventFilterKlass,
            CliStrings.CREATE_GATEWAYSENDER__GATEWAYEVENTFILTER));
      }
    }

    List<DeclarableType> gatewayTransportFilters = config.getGatewayTransportFilter();
    if (gatewayTransportFilters != null) {
      for (DeclarableType gatewayTransportFilter : gatewayTransportFilters) {
        Class gatewayTransportFilterKlass = forName(gatewayTransportFilter.getClassName(),
            CliStrings.CREATE_GATEWAYSENDER__GATEWAYTRANSPORTFILTER);
        gateway.addGatewayTransportFilter((GatewayTransportFilter) newInstance(
            gatewayTransportFilterKlass, CliStrings.CREATE_GATEWAYSENDER__GATEWAYTRANSPORTFILTER));
      }
    }
    return gateway.create(config.getId(), Integer.valueOf(config.getRemoteDistributedSystemId()));
  }

  @SuppressWarnings("unchecked")
  private static Class forName(String classToLoadName, String neededFor) {
    Class loadedClass = null;
    try {
      // Set Constraints
      ClassPathLoader classPathLoader = ClassPathLoader.getLatest();
      if (classToLoadName != null && !classToLoadName.isEmpty()) {
        loadedClass = classPathLoader.forName(classToLoadName);
      }
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(
          CliStrings.format(CliStrings.CREATE_REGION__MSG__COULD_NOT_FIND_CLASS_0_SPECIFIED_FOR_1,
              classToLoadName, neededFor),
          e);
    } catch (ClassCastException e) {
      throw new RuntimeException(CliStrings.format(
          CliStrings.CREATE_REGION__MSG__CLASS_SPECIFIED_FOR_0_SPECIFIED_FOR_1_IS_NOT_OF_EXPECTED_TYPE,
          classToLoadName, neededFor), e);
    }

    return loadedClass;
  }

  private static Object newInstance(Class klass, String neededFor) {
    Object instance = null;
    try {
      instance = klass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(CliStrings.format(
          CliStrings.CREATE_GATEWAYSENDER__MSG__COULD_NOT_INSTANTIATE_CLASS_0_SPECIFIED_FOR_1,
          klass, neededFor), e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(CliStrings.format(
          CliStrings.CREATE_GATEWAYSENDER__MSG__COULD_NOT_ACCESS_CLASS_0_SPECIFIED_FOR_1, klass,
          neededFor), e);
    }
    return instance;
  }

  @Override
  public String getId() {
    return ID;
  }

}
