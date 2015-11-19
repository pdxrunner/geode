/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.offheap;

/**
 * Used by tests to get notifications about the lifecycle of a 
 * SimpleMemoryAllocatorImpl.
 * 
 * @author Kirk Lund
 */
public interface LifecycleListener {
  /**
   * Callback is invoked after creating a new SimpleMemoryAllocatorImpl. 
   * 
   * Create occurs during the first initialization of an 
   * InternalDistributedSystem within the JVM.
   * 
   * @param allocator the instance that has just been created
   */
  public void afterCreate(SimpleMemoryAllocatorImpl allocator);
  /**
   * Callback is invoked after reopening an existing SimpleMemoryAllocatorImpl 
   * for reuse. 
   * 
   * Reuse occurs during any intialization of an 
   * InternalDistributedSystem after the first one was connected and then
   * disconnected within the JVM.
   * 
   * @param allocator the instance that has just been reopened for reuse
   */
  public void afterReuse(SimpleMemoryAllocatorImpl allocator);
  /**
   * Callback is invoked before closing the SimpleMemoryAllocatorImpl
   * 
   * Close occurs after the InternalDistributedSystem and DistributionManager 
   * have completely disconnected. 
   * 
   * @param allocator the instance that is about to be closed
   */
  public void beforeClose(SimpleMemoryAllocatorImpl allocator);
}