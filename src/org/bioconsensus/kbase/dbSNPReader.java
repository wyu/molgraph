package org.bioconsensus.kbase;

import org.ms2ms.graph.Graphs;
import org.ms2ms.math.Stats;
import org.ms2ms.utils.Strs;
import org.ms2ms.utils.TabFile;
import org.ms2ms.utils.Tools;
import toools.set.IntHashSet;
import toools.set.IntSet;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * ** Copyright 2014-2015 ms2ms.org
 * <p/>
 * Description:
 * <p/>
 * Author: wyu
 * Date:   6/8/15
 */
public class dbSNPReader extends TabReader
{
  public dbSNPReader(PropertyGraph g) { super(g); }
/*
  ##INFO=<ID=RS,Number=1,Type=Integer,Description="dbSNP ID (i.e. rs number)">
  ##INFO=<ID=RSPOS,Number=1,Type=Integer,Description="Chr position reported in dbSNP">
  ##INFO=<ID=RV,Number=0,Type=Flag,Description="RS orientation is reversed">
  ##INFO=<ID=VP,Number=1,Type=String,Description="Variation Property.  Documentation is at ftp://ftp.ncbi.nlm.nih.gov/snp/specs/dbSNP_BitField_latest.pdf">
  ##INFO=<ID=GENEINFO,Number=1,Type=String,Description="Pairs each of gene symbol:gene id.  The gene symbol and id are delimited by a colon (:) and each pair is delimited by a vertical bar (|)">
  ##INFO=<ID=dbSNPBuildID,Number=1,Type=Integer,Description="First dbSNP Build for RS">
  ##INFO=<ID=SAO,Number=1,Type=Integer,Description="Variant Allele Origin: 0 - unspecified, 1 - Germline, 2 - Somatic, 3 - Both">
  ##INFO=<ID=SSR,Number=1,Type=Integer,Description="Variant Suspect Reason Codes (may be more than one value added together) 0 - unspecified, 1 - Paralog, 2 - byEST, 4 - oldAlign, 8 - Para_EST, 16 - 1kg_failed, 1024 - other">
  ##INFO=<ID=WGT,Number=1,Type=Integer,Description="Weight, 00 - unmapped, 1 - weight 1, 2 - weight 2, 3 - weight 3 or more">
  ##INFO=<ID=VC,Number=1,Type=String,Description="Variation Class">
  ##INFO=<ID=PM,Number=0,Type=Flag,Description="Variant is Precious(Clinical,Pubmed Cited)">
  ##INFO=<ID=TPA,Number=0,Type=Flag,Description="Provisional Third Party Annotation(TPA) (currently rs from PHARMGKB who will give phenotype data)">
  ##INFO=<ID=PMC,Number=0,Type=Flag,Description="Links exist to PubMed Central article">
  ##INFO=<ID=S3D,Number=0,Type=Flag,Description="Has 3D structure - SNP3D table">
  ##INFO=<ID=SLO,Number=0,Type=Flag,Description="Has SubmitterLinkOut - From SNP->SubSNP->Batch.link_out">
  ##INFO=<ID=NSF,Number=0,Type=Flag,Description="Has non-synonymous frameshift A coding region variation where one allele in the set changes all downstream amino acids. FxnClass = 44">
  ##INFO=<ID=NSM,Number=0,Type=Flag,Description="Has non-synonymous missense A coding region variation where one allele in the set changes protein peptide. FxnClass = 42">
  ##INFO=<ID=NSN,Number=0,Type=Flag,Description="Has non-synonymous nonsense A coding region variation where one allele in the set changes to STOP codon (TER). FxnClass = 41">
  ##INFO=<ID=REF,Number=0,Type=Flag,Description="Has reference A coding region variation where one allele in the set is id entical to the reference sequence. FxnCode = 8">
  ##INFO=<ID=SYN,Number=0,Type=Flag,Description="Has synonymous A coding region variation where one allele in the set does not change the encoded amino acid. FxnCode = 3">
  ##INFO=<ID=U3,Number=0,Type=Flag,Description="In 3' UTR Location is in an untranslated region (UTR). FxnCode = 53">
  ##INFO=<ID=U5,Number=0,Type=Flag,Description="In 5' UTR Location is in an untranslated region (UTR). FxnCode = 55">
  ##INFO=<ID=ASS,Number=0,Type=Flag,Description="In acceptor splice site FxnCode = 73">
  ##INFO=<ID=DSS,Number=0,Type=Flag,Description="In donor splice-site FxnCode = 75">
  ##INFO=<ID=INT,Number=0,Type=Flag,Description="In Intron FxnCode = 6">
  ##INFO=<ID=R3,Number=0,Type=Flag,Description="In 3' gene region FxnCode = 13">
  ##INFO=<ID=R5,Number=0,Type=Flag,Description="In 5' gene region FxnCode = 15">
  ##INFO=<ID=OTH,Number=0,Type=Flag,Description="Has other variant with exactly the same set of mapped positions on NCBI refernce assembly.">
  ##INFO=<ID=CFL,Number=0,Type=Flag,Description="Has Assembly conflict. This is for weight 1 and 2 variant that maps to different chromosomes on different assemblies.">
  ##INFO=<ID=ASP,Number=0,Type=Flag,Description="Is Assembly specific. This is set if the variant only maps to one assembly">
  ##INFO=<ID=MUT,Number=0,Type=Flag,Description="Is mutation (journal citation, explicit fact): a low frequency variation that is cited in journal and other reputable sources">
  ##INFO=<ID=VLD,Number=0,Type=Flag,Description="Is Validated.  This bit is set if the variant has 2+ minor allele count based on frequency or genotype data.">
  ##INFO=<ID=G5A,Number=0,Type=Flag,Description=">5% minor allele frequency in each and all populations">
  ##INFO=<ID=G5,Number=0,Type=Flag,Description=">5% minor allele frequency in 1+ populations">
  ##INFO=<ID=HD,Number=0,Type=Flag,Description="Marker is on high density genotyping kit (50K density or greater).  The variant may have phenotype associations present in dbGaP.">
  ##INFO=<ID=GNO,Number=0,Type=Flag,Description="Genotypes available. The variant has individual genotype (in SubInd table).">
  ##INFO=<ID=KGPhase1,Number=0,Type=Flag,Description="1000 Genome phase 1 (incl. June Interim phase 1)">
  ##INFO=<ID=KGPhase3,Number=0,Type=Flag,Description="1000 Genome phase 3">
  ##INFO=<ID=CDA,Number=0,Type=Flag,Description="Variation is interrogated in a clinical diagnostic assay">
  ##INFO=<ID=LSD,Number=0,Type=Flag,Description="Submitted from a locus-specific database">
  ##INFO=<ID=MTP,Number=0,Type=Flag,Description="Microattribution/third-party annotation(TPA:GWAS,PAGE)">
  ##INFO=<ID=OM,Number=0,Type=Flag,Description="Has OMIM/OMIA">
  ##INFO=<ID=NOC,Number=0,Type=Flag,Description="Contig allele not present in variant allele list. The reference sequence allele at the mapped position is not present in the variant allele list, adjusted for orientation.">
  ##INFO=<ID=WTD,Number=0,Type=Flag,Description="Is Withdrawn by submitter If one member ss is withdrawn by submitter, then this bit is set.  If all member ss' are withdrawn, then the rs is deleted to SNPHistory">
  ##INFO=<ID=NOV,Number=0,Type=Flag,Description="Rs cluster has non-overlapping allele sets. True when rs set has more than 2 alleles from different submissions and these sets share no alleles in common.">
  ##FILTER=<ID=NC,Description="Inconsistent Genotype Submission For At Least One Sample">
  ##INFO=<ID=CAF,Number=.,Type=String,Description="An ordered, comma delimited list of allele frequencies based on 1000Genomes, starting with the reference allele followed by alternate alleles as ordered in the ALT column. Where a 1000Genomes alternate allele is not in the dbSNPs alternate allele set, the allele is added to the ALT column.  The minor allele is the second largest value in the list, and was previuosly reported in VCF as the GMAF.  This is the GMAF reported on the RefSNP and EntrezSNP pages and VariationReporter">
  ##INFO=<ID=COMMON,Number=1,Type=Integer,Description="RS is a common SNP.  A common SNP is one that has at least one 1000Genomes population with a minor allele of frequency >= 1% and for which 2 or more founders contribute to that minor allele frequency.">
*/

  synchronized public void parseDocument(String doc)
  {
    TabFile tab;
    try
    {
      // skip the lines starting with "##"
      tab = new TabFile(doc, TabFile.tabb, "##");
//      #CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
//      1	10177	rs367896724	A	AC	.	.	RS=367896724;RSPOS=10177;dbSNPBuildID=138;SSR=0;SAO=0;VP=0x050000020005140026000200;WGT=1;VC=DIV;R5;ASP;VLD;KGPhase3;CAF=0.5747,0.4253;COMMON=1
      while (tab.hasNext())
      {
        if (Strs.equals(tab.get("#CHROM"), "2")) continue;

        IntSet As = putNodes(Graphs.SNP, "ID", Strs.newMap('=', "#CHROM=" + Graphs.CHR + ":int", "POS=" + Graphs.CHR_POS + ":long"), tab, 'x');

        if (G.nodes%100000 ==0) System.out.print(".");
        if (G.nodes%5000000==0) System.out.println(G.nodes);

        if (Strs.isSet(tab.get("Info")))
          for (String info : Strs.split(tab.get("Info"), ';'))
            if (info.indexOf("GENEINFO=")==0)
            {
              // GENEINFO=AGRN:375790
              String gene = Strs.split(Strs.split(info, '=')[1], '.')[0];
              IntSet _G = G.putNodeByUIDType(Graphs.UID, gene, Graphs.TYPE, Graphs.GENE);
              G.putDirectedEdgesByUIDType(As, _G, null, Strs.newMap('=', Graphs.TYPE+"=belong_to"));
            }
            else if (info.indexOf("COMMON=")==0)
            {
              // GENEINFO=AGRN:375790
              G.setNodeLabelProperty(As, "Common:int", Strs.split(info, '=')[1]);
            }
      }
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
  }
/*
  private Set<String> parseGenes(Set<String> genes, String genestr, char linker, String... ignored)
  {
    if (!Strs.isSet(genestr)) return genes;
    if (genes==null) genes = new HashSet<>();

    String[] strs = Strs.split(genestr, ',', true);
    if (Tools.isSet(strs))
      for (String str : strs)
      {
        if (!Strs.isSet(str) || Strs.isA(str, ignored)) continue;
        Tools.add(genes, Strs.split(str, linker, true));
      }
    return genes;
  }
*/
}
