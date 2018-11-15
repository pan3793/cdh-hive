package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Cloudera specific test case for CDHMetaStoreSchemaInfo
 *
 */
public class TestCDHMetaStoreSchemaFactory {
  public HiveConf conf;

  @Before
  public void setup() {
    conf = new HiveConf(this.getClass());
  }

  @Test
  public void testWithCdhMetastoreSchemaInfo() {
    conf.set(HiveConf.ConfVars.METASTORE_SCHEMA_INFO_CLASS.varname,
      CDHMetaStoreSchemaInfo.class.getCanonicalName());
    IMetaStoreSchemaInfo metastoreSchemaInfo = MetaStoreSchemaInfoFactory.get(conf);
    Assert.assertNotNull(metastoreSchemaInfo);
    Assert.assertTrue("Unexpected instance type of the class MetaStoreSchemaInfo",
      metastoreSchemaInfo instanceof CDHMetaStoreSchemaInfo);
  }

  @Test
  public void testCdhDefault() {
    IMetaStoreSchemaInfo metastoreSchemaInfo = MetaStoreSchemaInfoFactory.get(conf);
    Assert.assertNotNull(metastoreSchemaInfo);
    Assert.assertTrue("Unexpected instance type of the class MetaStoreSchemaInfo",
      metastoreSchemaInfo instanceof CDHMetaStoreSchemaInfo);
  }

  // https://jira.cloudera.com/browse/CDH-75556
  @Test
  public void testCdhVersionChanges() {
    // Ensure the compareTo() method works properly.
    String [] versionStrings = {"1.1.0-cdh5.15.0", "1.1.0-cdh5.16.0", "2.1.0-cdh6.0.0",
        "2.1.0-cdh6.1.0", "2.1.0-cdh6.1.1"};
    CDHMetaStoreSchemaInfo.CDHVersion v1;
    CDHMetaStoreSchemaInfo.CDHVersion v2;
    for(int idx=0; idx<versionStrings.length-1; idx++) {
      v1 = new CDHMetaStoreSchemaInfo.CDHVersion(versionStrings[idx]);
      v2 = new CDHMetaStoreSchemaInfo.CDHVersion(versionStrings[idx+1]);
      Assert.assertEquals(v1.toString() + "->" + v2.toString(), -1, v1.compareTo(v2));
      Assert.assertEquals(v1.toString() + "->" + v2.toString(), 0, v1.compareTo(v1));
      Assert.assertEquals(v1.toString() + "->" + v2.toString(), 0, v2.compareTo(v2));
      Assert.assertEquals(v1.toString() + "->" + v2.toString(), 1, v2.compareTo(v1));
    }

    for(int idx=0; idx<versionStrings.length-1; idx++) {
      v1 = new CDHMetaStoreSchemaInfo.CDHVersion(versionStrings[idx], true);
      v2 = new CDHMetaStoreSchemaInfo.CDHVersion(versionStrings[idx+1], true);
      if (versionStrings[idx].contains("cdh6.1")) {
        Assert.assertEquals(v1.toString() + "->" + v2.toString(), 0, v1.compareTo(v2));
        Assert.assertEquals(v1.toString() + "->" + v2.toString(), 0, v1.compareTo(v1));
        Assert.assertEquals(v1.toString() + "->" + v2.toString(), 0, v2.compareTo(v2));
        Assert.assertEquals(v1.toString() + "->" + v2.toString(), 0, v2.compareTo(v1));
      } else {
        Assert.assertEquals(v1.toString() + "->" + v2.toString(), -1, v1.compareTo(v2));
        Assert.assertEquals(v1.toString() + "->" + v2.toString(), 0, v1.compareTo(v1));
        Assert.assertEquals(v1.toString() + "->" + v2.toString(), 0, v2.compareTo(v2));
        Assert.assertEquals(v1.toString() + "->" + v2.toString(), 1, v2.compareTo(v1));

      }
    }

    // Check for versions of the form cdh6.x
    v1 = new CDHMetaStoreSchemaInfo.CDHVersion("1.1.0-cdh5.15.0");
    v2 = new CDHMetaStoreSchemaInfo.CDHVersion("2.1.0-cdh6.x");
    Assert.assertEquals(v1.toString() + "->" + v2.toString(), -1, v1.compareTo(v2));
    Assert.assertEquals(v2.toString() + "->" + v1.toString(), 1, v2.compareTo(v1));

    v1 = new CDHMetaStoreSchemaInfo.CDHVersion("2.1.0-cdh6.1.0");
    v2 = new CDHMetaStoreSchemaInfo.CDHVersion("2.1.0-cdh6.x");
    Assert.assertEquals(v1.toString() + "->" + v2.toString(), 0, v1.compareTo(v2));
    Assert.assertEquals(v2.toString() + "->" + v1.toString(), 0, v2.compareTo(v1));
  }
}