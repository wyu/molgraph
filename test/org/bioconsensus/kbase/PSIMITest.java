package org.bioconsensus.kbase;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DirectedWeightedMultigraph;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;
import org.junit.Assert;
import org.junit.Test;
import org.ms2ms.graph.Graphs;
import org.ms2ms.graph.PropertyEdge;
import org.ms2ms.graph.PropertyNode;
import org.ms2ms.test.TestAbstract;
import psidev.psi.mi.xml.io.impl.PsimiXmlReader254;
import psidev.psi.mi.xml.model.EntrySet;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * ** Copyright 2014-2015 ms2ms.org
 * <p/>
 * Description:
 * <p/>
 * Author: wyu
 * Date:   1/2/15
 */
public class PSIMITest extends TestAbstract
{
  @Test
  public void readPSI25() throws Exception
  {
    File file = new File("/home/wyu/Projects/molgraph/data/10373512.xml");
    PsimiXmlReader254 reader = new PsimiXmlReader254();
    EntrySet es = reader.read( file );

    DirectedGraph<PropertyNode, PropertyEdge> graph = new SimpleDirectedWeightedGraph<>(PropertyEdge.class);
    Map<String, PropertyNode> tag_node = new HashMap<>();
    graph = Graphs.readPsiMI(graph, tag_node, es);
    Assert.assertNotNull(es);
  }
  @Test
  public void getInteractionGraph() throws Exception
  {

  }
}
