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
package com.gemstone.gemfire.distributed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import com.gemstone.gemfire.distributed.ServerLauncher.Builder;
import com.gemstone.gemfire.distributed.ServerLauncher.Command;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Integration tests for ServerLauncher class. These tests may require file system and/or network I/O.
 */
@Category(IntegrationTest.class)
public class ServerLauncherIntegrationJUnitTest {

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  
  @Rule
  public final TestName testName = new TestName();
  
  @Test
  public void testParseArguments() throws Exception {
    String rootFolder = this.temporaryFolder.getRoot().getCanonicalPath();
    Builder builder = new Builder();

    builder.parseArguments("start", "serverOne", "--assign-buckets", "--disable-default-server", "--debug", "--force",
      "--rebalance", "--redirect-output", "--dir=" + rootFolder, "--pid=1234",
        "--server-bind-address=" + InetAddress.getLocalHost().getHostAddress(), "--server-port=11235", "--hostname-for-clients=192.168.99.100");

    assertEquals(Command.START, builder.getCommand());
    assertEquals("serverOne", builder.getMemberName());
    assertEquals("192.168.99.100", builder.getHostNameForClients());
    assertTrue(builder.getAssignBuckets());
    assertTrue(builder.getDisableDefaultServer());
    assertTrue(builder.getDebug());
    assertTrue(builder.getForce());
    assertFalse(Boolean.TRUE.equals(builder.getHelp()));
    assertTrue(builder.getRebalance());
    assertTrue(builder.getRedirectOutput());
    assertEquals(rootFolder, builder.getWorkingDirectory());
    assertEquals(1234, builder.getPid().intValue());
    assertEquals(InetAddress.getLocalHost(), builder.getServerBindAddress());
    assertEquals(11235, builder.getServerPort().intValue());
  }

  @Test
  public void testSetAndGetWorkingDirectory() throws Exception {
    String rootFolder = this.temporaryFolder.getRoot().getCanonicalPath().toString();
    Builder builder = new Builder();

    assertEquals(ServerLauncher.DEFAULT_WORKING_DIRECTORY, builder.getWorkingDirectory());
    assertSame(builder, builder.setWorkingDirectory(rootFolder));
    assertEquals(rootFolder, builder.getWorkingDirectory());
    assertSame(builder, builder.setWorkingDirectory("  "));
    assertEquals(ServerLauncher.DEFAULT_WORKING_DIRECTORY, builder.getWorkingDirectory());
    assertSame(builder, builder.setWorkingDirectory(""));
    assertEquals(ServerLauncher.DEFAULT_WORKING_DIRECTORY, builder.getWorkingDirectory());
    assertSame(builder, builder.setWorkingDirectory(null));
    assertEquals(ServerLauncher.DEFAULT_WORKING_DIRECTORY, builder.getWorkingDirectory());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetWorkingDirectoryToNonExistingDirectory() {
    try {
      new Builder().setWorkingDirectory("/path/to/non_existing/directory");
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_WORKING_DIRECTORY_NOT_FOUND_ERROR_MESSAGE
        .toLocalizedString("Server"), expected.getMessage());
      assertTrue(expected.getCause() instanceof FileNotFoundException);
      assertEquals("/path/to/non_existing/directory", expected.getCause().getMessage());
      throw expected;
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetWorkingDirectoryToFile() throws IOException {
    File tmpFile = File.createTempFile("tmp", "file");

    assertNotNull(tmpFile);
    assertTrue(tmpFile.isFile());

    tmpFile.deleteOnExit();

    try {
      new Builder().setWorkingDirectory(tmpFile.getAbsolutePath());
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_WORKING_DIRECTORY_NOT_FOUND_ERROR_MESSAGE
        .toLocalizedString("Server"), expected.getMessage());
      assertTrue(expected.getCause() instanceof FileNotFoundException);
      assertEquals(tmpFile.getAbsolutePath(), expected.getCause().getMessage());
      throw expected;
    }
  }

  @Test
  public void testBuild() throws Exception {
    String rootFolder = this.temporaryFolder.getRoot().getCanonicalPath().toString();
    
    ServerLauncher launcher = new Builder()
      .setCommand(Command.STOP)
      .setAssignBuckets(true)
      .setForce(true)
      .setMemberName("serverOne")
      .setRebalance(true)
      .setServerBindAddress(InetAddress.getLocalHost().getHostAddress())
      .setServerPort(11235)
      .setWorkingDirectory(rootFolder)
      .setCriticalHeapPercentage(90.0f)
      .setEvictionHeapPercentage(75.0f)
      .setMaxConnections(100)
      .setMaxMessageCount(512)
      .setMaxThreads(8)
      .setMessageTimeToLive(120000)
      .setSocketBufferSize(32768)
      .build();

    assertNotNull(launcher);
    assertTrue(launcher.isAssignBuckets());
    assertFalse(launcher.isDebugging());
    assertFalse(launcher.isDisableDefaultServer());
    assertTrue(launcher.isForcing());
    assertFalse(launcher.isHelping());
    assertTrue(launcher.isRebalancing());
    assertFalse(launcher.isRunning());
    assertEquals(Command.STOP, launcher.getCommand());
    assertEquals("serverOne", launcher.getMemberName());
    assertEquals(InetAddress.getLocalHost(), launcher.getServerBindAddress());
    assertEquals(11235, launcher.getServerPort().intValue());
    assertEquals(rootFolder, launcher.getWorkingDirectory());
    assertEquals(90.0f, launcher.getCriticalHeapPercentage().floatValue(), 0.0f);
    assertEquals(75.0f, launcher.getEvictionHeapPercentage().floatValue(), 0.0f);
    assertEquals(100, launcher.getMaxConnections().intValue());
    assertEquals(512, launcher.getMaxMessageCount().intValue());
    assertEquals(8, launcher.getMaxThreads().intValue());
    assertEquals(120000, launcher.getMessageTimeToLive().intValue());
    assertEquals(32768, launcher.getSocketBufferSize().intValue());
  }

  @Test
  public void testBuildWithMemberNameSetInGemFirePropertiesOnStart() throws Exception {
    File propertiesFile = new File(this.temporaryFolder.getRoot().getCanonicalPath(), "gemfire.properties");
    System.setProperty(DistributedSystem.PROPERTIES_FILE_PROPERTY, propertiesFile.getCanonicalPath());

    Properties gemfireProperties = new Properties();
    gemfireProperties.setProperty(DistributionConfig.NAME_NAME, "server123");
    gemfireProperties.store(new FileWriter(propertiesFile, false), this.testName.getMethodName());

    assertTrue(propertiesFile.isFile());
    assertTrue(propertiesFile.exists());

    ServerLauncher launcher = new Builder()
      .setCommand(ServerLauncher.Command.START)
      .setMemberName(null)
      .build();

    assertNotNull(launcher);
    assertEquals(ServerLauncher.Command.START, launcher.getCommand());
    assertNull(launcher.getMemberName());
  }
  
  @Test(expected = IllegalStateException.class)
  public void testBuildWithInvalidWorkingDirectoryOnStart() throws Exception {
    try {
      new Builder().setCommand(Command.START)
        .setMemberName("serverOne")
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath().toString())
        .build();
    }
    catch (IllegalStateException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_WORKING_DIRECTORY_OPTION_NOT_VALID_ERROR_MESSAGE
        .toLocalizedString("Server"), expected.getMessage());
      throw expected;
    }
  }

  protected File writeGemFirePropertiesToFile(final Properties gemfireProperties, final String filename, final String comment) {
    try {
      final File gemfirePropertiesFile = this.temporaryFolder.newFile(filename);
      gemfireProperties.store(new FileWriter(gemfirePropertiesFile, false), comment);
      return gemfirePropertiesFile;
    }
    catch (IOException e) {
      return null;
    }
  }
}
