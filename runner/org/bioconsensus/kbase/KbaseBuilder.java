package org.bioconsensus.kbase;

import org.junit.Before;
import org.junit.Test;
import org.ms2ms.r.Dataframe;
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
  @Test
  public void getInteractionGraph() throws Exception
  {
    String ibd = "IntAct/psi25/datasets/IBD",
        dataset = "IntAct/psi25/datasets",
        psi25 = "IntAct/psi25";

    PsiMI25Reader intact = GraphHandler.build(kb + date + ".intact", root, ibd);
    GraphHandler.ESGN2Gene(intact.G, new Dataframe(root+"HGNC_20150221.mapping", '\t'));

  System.out.println(intact.G.inventory());
  }
}
