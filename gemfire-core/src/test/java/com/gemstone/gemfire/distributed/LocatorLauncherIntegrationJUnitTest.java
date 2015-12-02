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

import com.gemstone.gemfire.distributed.LocatorLauncher.Builder;
import com.gemstone.gemfire.distributed.LocatorLauncher.Command;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Integration tests for LocatorLauncher. These tests require file system I/O.
 */
@Category(IntegrationTest.class)
public class LocatorLauncherIntegrationJUnitTest {

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  
  @Rule
  public final TestName testName = new TestName();
  
  @Test
  public void testBuilderParseArguments() throws Exception {
    String expectedWorkingDirectory = this.temporaryFolder.getRoot().getCanonicalPath();
    Builder builder = new Builder();

    builder.parseArguments("start", "memberOne", "--bind-address", InetAddress.getLocalHost().getHostAddress(),
      "--dir", expectedWorkingDirectory, "--hostname-for-clients", "Tucows", "--pid", "1234", "--port", "11235",
        "--redirect-output", "--force", "--debug");

    assertEquals(Command.START, builder.getCommand());
    assertEquals(InetAddress.getLocalHost(), builder.getBindAddress());
    assertEquals(expectedWorkingDirectory, builder.getWorkingDirectory());
    assertEquals("Tucows", builder.getHostnameForClients());
    assertEquals(1234, builder.getPid().intValue());
    assertEquals(11235, builder.getPort().intValue());
    assertTrue(builder.getRedirectOutput());
    assertTrue(builder.getForce());
    assertTrue(builder.getDebug());
  }

  @Test
  public void testBuilderParseArgumentsWithCommandInArguments() throws Exception {
    String expectedWorkingDirectory = this.temporaryFolder.getRoot().getCanonicalPath();
    Builder builder = new Builder();

    builder.parseArguments("start", "--dir=" + expectedWorkingDirectory, "--port", "12345", "memberOne");

    assertEquals(Command.START, builder.getCommand());
    assertFalse(Boolean.TRUE.equals(builder.getDebug()));
    assertFalse(Boolean.TRUE.equals(builder.getForce()));
    assertFalse(Boolean.TRUE.equals(builder.getHelp()));
    assertNull(builder.getBindAddress());
    assertNull(builder.getHostnameForClients());
    assertEquals("12345", builder.getMemberName());
    assertNull(builder.getPid());
    assertEquals(expectedWorkingDirectory, builder.getWorkingDirectory());
    assertEquals(12345, builder.getPort().intValue());
  }

  @Test
  public void testBuildWithMemberNameSetInGemfirePropertiesOnStart() throws Exception {
    File propertiesFile = new File(this.temporaryFolder.getRoot().getCanonicalPath(), "gemfire.properties");
    System.setProperty(DistributedSystem.PROPERTIES_FILE_PROPERTY, propertiesFile.getCanonicalPath());
    
    Properties gemfireProperties = new Properties();
    gemfireProperties.setProperty(DistributionConfig.NAME_NAME, "locator123");
    gemfireProperties.store(new FileWriter(propertiesFile, false), this.testName.getMethodName());
    assertTrue(propertiesFile.isFile());
    assertTrue(propertiesFile.exists());

    LocatorLauncher launcher = new Builder().setCommand(Command.START).setMemberName(null).build();

    assertNotNull(launcher);
    assertEquals(Command.START, launcher.getCommand());
    assertNull(launcher.getMemberName());
  }

  @Test
  public void testSetAndGetWorkingDirectory() throws Exception {
    String rootFolder = this.temporaryFolder.getRoot().getCanonicalPath();
    Builder builder = new Builder();

    assertEquals(AbstractLauncher.DEFAULT_WORKING_DIRECTORY, builder.getWorkingDirectory());
    assertSame(builder, builder.setWorkingDirectory(null));
    assertEquals(AbstractLauncher.DEFAULT_WORKING_DIRECTORY, builder.getWorkingDirectory());
    assertSame(builder, builder.setWorkingDirectory(""));
    assertEquals(AbstractLauncher.DEFAULT_WORKING_DIRECTORY, builder.getWorkingDirectory());
    assertSame(builder, builder.setWorkingDirectory("  "));
    assertEquals(AbstractLauncher.DEFAULT_WORKING_DIRECTORY, builder.getWorkingDirectory());
    assertSame(builder, builder.setWorkingDirectory(rootFolder));
    assertEquals(rootFolder, builder.getWorkingDirectory());
    assertSame(builder, builder.setWorkingDirectory(null));
    assertEquals(AbstractLauncher.DEFAULT_WORKING_DIRECTORY, builder.getWorkingDirectory());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetWorkingDirectoryToFile() throws IOException {
    File tmpFile = File.createTempFile("tmp", "file");

    assertNotNull(tmpFile);
    assertTrue(tmpFile.isFile());

    tmpFile.deleteOnExit();

    try {
      new Builder().setWorkingDirectory(tmpFile.getCanonicalPath());
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_WORKING_DIRECTORY_NOT_FOUND_ERROR_MESSAGE
        .toLocalizedString("Locator"), expected.getMessage());
      assertTrue(expected.getCause() instanceof FileNotFoundException);
      assertEquals(tmpFile.getCanonicalPath(), expected.getCause().getMessage());
      throw expected;
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetWorkingDirectoryToNonExistingDirectory() {
    try {
      new Builder().setWorkingDirectory("/path/to/non_existing/directory");
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_WORKING_DIRECTORY_NOT_FOUND_ERROR_MESSAGE
        .toLocalizedString("Locator"), expected.getMessage());
      assertTrue(expected.getCause() instanceof FileNotFoundException);
      assertEquals("/path/to/non_existing/directory", expected.getCause().getMessage());
      throw expected;
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testBuildWithNoMemberNameOnStart() throws Exception {
    System.setProperty("user.dir", this.temporaryFolder.getRoot().getCanonicalPath());
    try {
      new Builder().setCommand(Command.START).build();
    }
    catch (IllegalStateException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_MEMBER_NAME_VALIDATION_ERROR_MESSAGE.toLocalizedString("Locator"),
        expected.getMessage());
      throw expected;
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testBuildWithMismatchingCurrentAndWorkingDirectoryOnStart() throws Exception {
    try {
      new Builder().setCommand(Command.START)
        .setMemberName("memberOne")
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    }
    catch (IllegalStateException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_WORKING_DIRECTORY_OPTION_NOT_VALID_ERROR_MESSAGE
        .toLocalizedString("Locator"), expected.getMessage());
      throw expected;
    }
  }
}
