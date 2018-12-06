package org.apache.geode.management.internal.cli.commands;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class StatusClusterConfigServiceCommandDUnitTest {
  private static MemberVM locator1;

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    locator1 = cluster.startLocatorVM(0, 0);
    cluster.startLocatorVM(1, locator1.getPort());

    gfsh.connectAndVerify(locator1);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    gfsh.connectAndVerify(locator1);
    gfsh.execute("shutdown --include-locators");
  }

  @Test
  public void testStatusClusterConfigService() {
    gfsh.executeAndAssertThat("status cluster-config-service")
        .statusIsSuccess()
        .tableHasRowWithValues("Name", "Status", "locator-0", "RUNNING")
        .tableHasRowWithValues("Name", "Status", "locator-1", "RUNNING");
  }
}
