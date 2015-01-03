package org.bioconsensus.kbase;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TransactionBuilder;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import org.junit.Test;
import org.ms2ms.graph.Graphs;
import org.ms2ms.nosql.Titans;
import org.ms2ms.test.TestAbstract;

/**
 * ** Copyright 2014-2015 ms2ms.org
 * <p/>
 * Description:
 * <p/>
 * Author: wyu
 * Date:   12/21/14
 */
public class UniprotTest01 extends TestAbstract
{
  @Test
  public void readUniprot() throws Exception
  {
    TitanGraph g = Titans.openHBaseGraph();
//    TransactionBuilder tx = g.buildTransaction();
//    tx.enableBatchLoading();
//    TitanTransaction t = tx.start();
    g.shutdown(); TitanCleanup.clear(g);
//    Graphs.clear(g);

    g = Titans.openHBaseGraph();
    UniprotHandler uniprot = new UniprotHandler(g, "Homo sapiens");
    uniprot.parseDocument("/home/wyu/Projects/molgraph/data/up1k.xml");
//    uniprot.parseDocument("/media/data/import/bio4j/uniprot_sprot.xml");
    g.shutdown();
  }

  @Test
  public void readMiTAB() throws Exception
  {

  }

}
