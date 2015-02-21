package org.bioconsensus.kbase;

import org.junit.Before;
import org.junit.Test;
import org.ms2ms.test.TestAbstract;

import java.util.Date;

/** Copyright 2014-2015 ms2ms.org
 * <p/>
 * Description: This is the place where we build the kbase from various data sources
 *
 * Each data source is processed separately and saved into its own file.
 * They are assembled together at the end.
 *
 * <p/>
 * Author: wyu
 * Date:   2/21/15
 */
public class KbaseBuilder extends TestAbstract
{
  String root = "/media/data/import/", kb="/media/data/kbase/", date;

  @Before
  public void setUp()
  {
    Date d = new Date(); date=d.getYear()+d.getMonth()+d.getDate()+"";
  }
  @Test
  public void readBioGRIDGraph() throws Exception
  {
    PsiMI25Reader biogrid = GraphHandler.build(kb+date+".BioGRID", root, "BioGRID/BIOGRID-ALL-3.2.120.psi25.xml");
    System.out.println(biogrid.G.inventory());
  }
}
