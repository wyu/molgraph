package org.bioconsensus.kbase;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.ms2ms.graph.Graphs;
import org.ms2ms.graph.PropertyEdge;
import org.ms2ms.graph.PropertyNode;
import org.ms2ms.nosql.Titans;
import org.ms2ms.r.Dataframe;
import org.ms2ms.test.TestAbstract;
import psidev.psi.mi.xml.PsimiXmlLightweightReader;
import psidev.psi.mi.xml.io.impl.PsimiXmlReader254;
import psidev.psi.mi.xml.model.Entry;
import psidev.psi.mi.xml.model.EntrySet;
import psidev.psi.mi.xml.xmlindex.IndexedEntry;
import psidev.psi.mi.xml.xmlindex.impl.PsimiXmlPullParser253;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ** Copyright 2014-2015 ms2ms.org
 * <p/>
 * Description:
 * <p/>
 * Author: wyu
 * Date:   1/30/15
 */
public class GraphReaderTest extends TestAbstract
{
  @Before
  public void setUp()
  {
    PsiMI25Reader.mapping = new Dataframe("/media/data/import/HGNC_20150221.mapping", '\t');
    PsiMI25Reader.es2gene = PsiMI25Reader.mapping.toMap("Ensembl Gene ID", "Approved Symbol");
  }

  @Test
  public void readComboGraph() throws Exception
  {
    String ibd = "/media/data/import/IntAct/psi25/datasets/IBD",
        dataset = "/media/data/import/IntAct/psi25/datasets",
        psi25 = "/media/data/import/IntAct/psi25";

    PsiMI25Reader interact = new PsiMI25Reader();
    interact.readRecursive(dataset);

    System.out.println(interact.G.inventory());

    GWASReader g = new GWASReader(interact.G);
//    GWASReader g = new GWASReader(new PropertyGraph());
    g.parseDocument("/media/data/import/GWAS/gwas_catalog_v1.0.1-downloaded_2015-04-08.tsv");
    System.out.println(g.G.inventory());

    DrugBankReader dbank = new DrugBankReader(g.G);
    dbank.read("/media/data/import/drugbank/drugbank.xml");
    System.out.println(dbank.G.inventory());

    GTExReader gteX = new GTExReader(dbank.G);
    gteX.readRecursive("/media/data/import/eQTL/GTEx/2014-01-17/");
    System.out.println(gteX.G.inventory());

    DisGeNETReader disease = new DisGeNETReader(gteX.G);
    disease.readRecursive("/media/data/import/DisGeNET/all_gene_disease_associations.txt");
    System.out.println(disease.G.inventory());

    disease.G.write("/usr/local/neo4j/current/import/Combined.grph");

    disease.G.writeBatch("/usr/local/neo4j/current/import/Combined", "combined.db");
  }

  @Test
  public void getTTD() throws Exception
  {
    String trans = "/media/data/import/TTD";

    TTDReader g = new TTDReader(new PropertyGraph());
    g.parseDocument(trans);
    System.out.println(g.G.inventory());
  }
  @Test
  public void getEBI_GWAS() throws Exception
  {
    GWASReader g = new GWASReader(new PropertyGraph());
//    g.parseDocument("/home/wyu/Projects/molgraph/data/gwas_ebi1000.tsv");
    g.parseDocument("/media/data/import/GWAS/gwas_catalog_v1.0.1-downloaded_2015-04-08.tsv");
    System.out.println(g.G.inventory());

    g.G.writeNodes2CSVByLabel("/usr/local/neo4j/current/import/GWAS");
    g.G.writeEdges2CSVByType("/usr/local/neo4j/current/import/GWAS");
//    g.G.writeNodes2CSV("/usr/local/neo4j/current/import/GWAS");
//    g.G.writeEdges2CSV("/usr/local/neo4j/current/import/GWAS");
    //g.G.write("/tmp/GWAS.20150407");
  }

  @Test
  public void readDrugBankGraph() throws Exception
  {
    DrugBankReader dbank = new DrugBankReader();
    dbank.parseDocument("/media/data/import/drugbank/drugbank.xml");
    System.out.println(dbank.G.inventory());

    dbank.G.writeNodes2CSVByLabel("/usr/local/neo4j/current/import/DrugBank");
    dbank.G.writeEdges2CSVByType( "/usr/local/neo4j/current/import/DrugBank");
  }

  @Test
  public void readBioGRIDGraph() throws Exception
  {
//    PsiMI25Reader biogrid = GraphHandler.read("/media/data/import/BioGRID/BIOGRID-ALL-3.2.120.psi25.xml");
//    uniprot.parseDocument("/media/data/import/bio4j/uniprot_sprot.xml");
    BioGRIDReader biogrid = new BioGRIDReader(new PropertyGraph());

    biogrid.parseDocuments("/media/data/import/BioGRID/BIOGRID-ALL-3.2.120.psi25.xml");
    System.out.println(biogrid.G.inventory());
    biogrid.G.write("/tmp/BioGRID.20150408");
  }
/*
  @Test
  public void readIntActGraph() throws Exception
  {
    PsiMI25Reader interact = PsiMI25Reader.read(null, "/home/wyu/Projects/molgraph/data/IBD25407307.xml");
  }
*/
  @Test
  public void getInteractionGraph() throws Exception
  {
    String ibd = "/media/data/import/IntAct/psi25/datasets/IBD",
       dataset = "/media/data/import/IntAct/psi25/datasets",
         psi25 = "/media/data/import/IntAct/psi25";

    PsiMI25Reader interact = new PsiMI25Reader();
    interact.readRecursive(dataset);

//    interact.G.write("/tmp/IBD02");
    System.out.println(interact.G.inventory());

    interact.G.writeBatch("/usr/local/neo4j/current/import/IntActBatch", "intact.db");
  }
  @Test
  public void getDisGeNET() throws Exception
  {
    DisGeNETReader g = new DisGeNETReader(new PropertyGraph());
    g.readRecursive("/media/data/import/DisGeNET/all_gene_disease_associations.txt");
    System.out.println(g.G.inventory());

    g.G.writeNodes2CSVByLabel("/usr/local/neo4j/current/import/DisGeNET");
    g.G.writeEdges2CSVByType( "/usr/local/neo4j/current/import/DisGeNET");

    System.out.println();
  }
  @Test
  public void getGTEx() throws Exception
  {
    String gteX = "/media/data/import/eQTL/GTEx/2014-01-17/";

    GTExReader g = new GTExReader(new PropertyGraph());
    g.readRecursive(gteX);
    System.out.println(g.G.inventory());

    g.G.writeNodes2CSVByLabel("/usr/local/neo4j/current/import/GTEx");
    g.G.writeEdges2CSVByType( "/usr/local/neo4j/current/import/GTEx");
  }
  @Test
  public void getGeneNetworkNL() throws Exception
  {
    String trans = "2012-12-21-TransEQTLsFDR0.5.txt",
             cis = "2012-12-21-CisAssociationsProbeLevelFDR0.5.txt",
//            root = "/home/wyu/Projects/molgraph/data/";
            root = "/media/data/import/eQTL/genenetwork.nl/";

    GeneNetworkNLReader g = new GeneNetworkNLReader(new PropertyGraph());
    g.parseDocument(root+trans);
    System.out.println(g.G.inventory());

    g.parseDocument(root+cis);
    System.out.println(g.G.inventory());

    System.out.println();
  }
  /** prepare the databases before running the test  **/
  // /usr/local/hbase/4titan$ bin/start-hbase.sh
  // sudo /etc/init.d/elasticsearch start

  @Test
  public void readBioGRID() throws Exception
  {
    TitanGraph g = Titans.openHBaseGraph();
    g.shutdown(); TitanCleanup.clear(g);

    g = Titans.openHBaseGraph();
    Psi25Handler biogrid = new Psi25Handler(g);
    biogrid.parseDocument("/media/data/import/BioGRID/BIOGRID-ALL-3.2.120.psi25.xml");
//    uniprot.parseDocument("/media/data/import/bio4j/uniprot_sprot.xml");
    g.shutdown();
  }

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
  public void StreamParser_253_file() throws Exception
  {
    FileInputStream file = new FileInputStream(new File("/media/data/import/BioGRID/BIOGRID-ALL-3.2.120.psi25.xml"));
    PsimiXmlPullParser253 reader = new PsimiXmlPullParser253();

    for (int i=0; i<10; i++)
    {
      Entry E = reader.parseEntry(file);
      System.out.println(E.hasAttributes());
    }
  }

  @Test
  public void getIndexedEntries_253_file() throws Exception
  {
    File file = new File("/media/data/import/BioGRID/BIOGRID-ALL-3.2.120.psi25.xml");
    PsimiXmlLightweightReader reader = new PsimiXmlLightweightReader( file );

    final List<IndexedEntry> indexedEntries = reader.getIndexedEntries();
    for (IndexedEntry iE : indexedEntries)
    {
      Entry E = iE.unmarshalledEntry();
      System.out.println(E.hasAttributes());
    }
    Assert.assertNotNull(indexedEntries);
    Assert.assertEquals( 1, indexedEntries.size() );
  }
}
