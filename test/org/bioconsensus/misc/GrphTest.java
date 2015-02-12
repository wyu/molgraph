package org.bioconsensus.misc;

import grph.Grph;
import grph.in_memory.InMemoryGrph;
import org.junit.Before;
import org.junit.Test;
import org.ms2ms.test.TestAbstract;

/**
 * ** Copyright 2014-2015 ms2ms.org
 * <p/>
 * Description:
 * <p/>
 * Author: wyu
 * Date:   2/4/15
 */
public class GrphTest extends TestAbstract
{
  @Before
  public void setUp()
  {
    ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
  }

  @Test
  public void simple()
  {
    Grph g = new InMemoryGrph();
    int n1=g.addVertex(), n2=g.addVertex();
    g.addUndirectedSimpleEdge(n1, n2);

    g.display();
  }

}
