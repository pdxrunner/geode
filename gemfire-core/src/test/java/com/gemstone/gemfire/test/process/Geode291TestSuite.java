package com.gemstone.gemfire.test.process;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.gemstone.gemfire.distributed.AbstractLauncherIntegrationJUnitTest;
import com.gemstone.gemfire.distributed.AbstractLauncherJUnitTest;
import com.gemstone.gemfire.distributed.AbstractLauncherServiceStatusJUnitTest;
import com.gemstone.gemfire.distributed.LauncherMemberMXBeanJUnitTest;
import com.gemstone.gemfire.distributed.LocatorLauncherIntegrationJUnitTest;
import com.gemstone.gemfire.distributed.LocatorLauncherJUnitTest;
import com.gemstone.gemfire.distributed.LocatorLauncherLocalFileJUnitTest;
import com.gemstone.gemfire.distributed.LocatorLauncherLocalJUnitTest;
import com.gemstone.gemfire.distributed.LocatorLauncherRemoteFileJUnitTest;
import com.gemstone.gemfire.distributed.LocatorLauncherRemoteJUnitTest;
import com.gemstone.gemfire.distributed.ServerLauncherIntegrationJUnitTest;
import com.gemstone.gemfire.distributed.ServerLauncherJUnitTest;
import com.gemstone.gemfire.distributed.ServerLauncherLocalFileJUnitTest;
import com.gemstone.gemfire.distributed.ServerLauncherLocalJUnitTest;
import com.gemstone.gemfire.distributed.ServerLauncherRemoteFileJUnitTest;
import com.gemstone.gemfire.distributed.ServerLauncherRemoteJUnitTest;
import com.gemstone.gemfire.distributed.ServerLauncherWithSpringJUnitTest;
import com.gemstone.gemfire.test.golden.FailWithErrorInOutputJUnitTest;
import com.gemstone.gemfire.test.golden.FailWithExtraLineInOutputJUnitTest;
import com.gemstone.gemfire.test.golden.FailWithLineMissingFromEndOfOutputJUnitTest;
import com.gemstone.gemfire.test.golden.FailWithLineMissingFromMiddleOfOutputJUnitTest;
import com.gemstone.gemfire.test.golden.FailWithLoggerErrorInOutputJUnitTest;
import com.gemstone.gemfire.test.golden.FailWithLoggerFatalInOutputJUnitTest;
import com.gemstone.gemfire.test.golden.FailWithLoggerWarnInOutputJUnitTest;
import com.gemstone.gemfire.test.golden.FailWithSevereInOutputJUnitTest;
import com.gemstone.gemfire.test.golden.FailWithTimeoutOfWaitForOutputToMatchJUnitTest;
import com.gemstone.gemfire.test.golden.FailWithWarningInOutputJUnitTest;
import com.gemstone.gemfire.test.golden.PassJUnitTest;
import com.gemstone.gemfire.test.golden.PassWithExpectedErrorJUnitTest;
import com.gemstone.gemfire.test.golden.PassWithExpectedSevereJUnitTest;
import com.gemstone.gemfire.test.golden.PassWithExpectedWarningJUnitTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  MainLauncherJUnitTest.class,
  ProcessWrapperJUnitTest.class,
  FailWithErrorInOutputJUnitTest.class,
  FailWithExtraLineInOutputJUnitTest.class,
  FailWithLineMissingFromEndOfOutputJUnitTest.class,
  FailWithLineMissingFromMiddleOfOutputJUnitTest.class,
  FailWithLoggerErrorInOutputJUnitTest.class,
  FailWithLoggerFatalInOutputJUnitTest.class,
  FailWithLoggerWarnInOutputJUnitTest.class,
  FailWithSevereInOutputJUnitTest.class,
  FailWithTimeoutOfWaitForOutputToMatchJUnitTest.class,
  FailWithWarningInOutputJUnitTest.class,
  PassJUnitTest.class,
  PassWithExpectedErrorJUnitTest.class,
  PassWithExpectedSevereJUnitTest.class,
  PassWithExpectedWarningJUnitTest.class,
  AbstractLauncherJUnitTest.class,
  AbstractLauncherIntegrationJUnitTest.class,
  AbstractLauncherServiceStatusJUnitTest.class,
  LauncherMemberMXBeanJUnitTest.class,
  LocatorLauncherJUnitTest.class,
  LocatorLauncherIntegrationJUnitTest.class,
  LocatorLauncherLocalJUnitTest.class,
  LocatorLauncherLocalFileJUnitTest.class,
  LocatorLauncherRemoteJUnitTest.class,
  LocatorLauncherRemoteFileJUnitTest.class,
  ServerLauncherJUnitTest.class,
  ServerLauncherIntegrationJUnitTest.class,
  ServerLauncherLocalJUnitTest.class,
  ServerLauncherLocalFileJUnitTest.class,
  ServerLauncherRemoteJUnitTest.class,
  ServerLauncherRemoteFileJUnitTest.class,
  ServerLauncherWithSpringJUnitTest.class,
})
public class Geode291TestSuite {
}
