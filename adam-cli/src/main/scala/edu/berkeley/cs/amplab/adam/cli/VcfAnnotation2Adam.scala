/*
 * Copyright (c) 2013. The Broad Institute of MIT/Harvard
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.cs.amplab.adam.cli

import java.io.File;

import edu.berkeley.cs.amplab.adam.models.ADAMVariantContext
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.rdd.variation.ADAMVariationContext._
import edu.berkeley.cs.amplab.adam.util.ParquetLogger
import java.util.logging.Level
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.RDD
import org.kohsuke.args4j.Argument
import edu.berkeley.cs.amplab.adam.converters.VariantContextConverter
import fi.tkk.ics.hadoop.bam.{VariantContextWritable, VCFInputFormat}
import org.apache.hadoop.io.LongWritable
import parquet.hadoop.util.ContextUtil
import org.broadinstitute.variant.variantcontext.VariantContext
import edu.berkeley.cs.amplab.adam.avro.{ADAMDatabaseVariantAnnotation, ADAMOMIMVariantAnnotation, ADAMClinvarVariantAnnotation}

object VcfAnnotation2Adam extends AdamCommandCompanion {

  val commandName = "anno2adam"
  val commandDescription = "Convert a annotation file (in VCF format) to the corresponding ADAM format"

  def apply(cmdLine: Array[String]) = {
    new VcfAnnotation2Adam(Args4j[VcfAnnotation2AdamArgs](cmdLine))
  }
}

class VcfAnnotation2AdamArgs extends Args4jBase with ParquetArgs with SparkArgs {
  @Argument(required = true, metaVar = "VCF", usage = "The VCF file to convert", index = 0)
  var vcfFile: String = _
  @Argument(required = true, metaVar = "ADAM", usage = "Location to write ADAM Variant data", index = 1)
  var outputPath: String = null
}

class VcfAnnotation2Adam(val args: VcfAnnotation2AdamArgs) extends AdamSparkCommand[VcfAnnotation2AdamArgs] with Logging {
  val companion = VcfAnnotation2Adam

  def run(sc: SparkContext, job: Job) {
    log.info("Reading VCF file from %s".format(args.vcfFile))
    val job = Job.getInstance(sc.hadoopConfiguration)
    val vcc = new VariantContextConverter
    val records = sc.newAPIHadoopFile(
      args.vcfFile,
      classOf[VCFInputFormat], classOf[LongWritable], classOf[VariantContextWritable],
      ContextUtil.getConfiguration(job)
    )

    def createAnnotation(vc : VariantContext) : Option[ADAMDatabaseVariantAnnotation] =
    {
      val avc = vcc.convert(vc)

      if (avc.size > 0 )
      {

        val anno = ADAMDatabaseVariantAnnotation.newBuilder()
          .setVariant(avc(0).variant)
          .setClinvar(ADAMClinvarAnnotation(vc))
          .build()

        anno
      }
      None
    }

    val annotations = records.map( vc => createAnnotation(vc._2.get)).filter(_.isDefined).map(_.get)

    annotations.adamSave(args.outputPath, blockSize = args.blockSize, pageSize = args.pageSize,
      compressCodec = args.compressionCodec, disableDictionaryEncoding = args.disableDictionary)

    log.info("Converted %d records".format(records.count()))

  }

}

object ADAMClinvarAnnotation
{

  def apply(vc : VariantContext)  :  ADAMClinvarVariantAnnotation  =
  {
    val clinvar = ADAMClinvarVariantAnnotation.newBuilder
      .setClinicalSignificance(vc.getAttributeAsString("CS", ""))
      .setRsDbsnp(vc.getAttributeAsInt("RS", -1))
//    .setReviewstatus(vc.getAttribute("RV") )
//    .setvc.getAttribute("VP")
      .setGeneSymbol(vc.getAttributeAsString("GENEINFO", ""))
//    .setNsvDbvar(vc.getAttribute("dbSNPBuildID"))
//    vc.getAttribute("SAO")
//    vc.getAttribute("SSR")
//    vc.getAttribute("WGT")
//    vc.getAttribute("VC")
      .setClinicalSignificance(vc.getAttributeAsString("PM", ""))
//    vc.getAttribute("TPA")
//    vc.getAttribute("PMC")
//    vc.getAttribute("S3D")
//    vc.getAttribute("SLO")
//    vc.getAttribute("NSF")
//    vc.getAttribute("NSM")
//    vc.getAttribute("NSN")
//    vc.getAttribute("REF")
//    vc.getAttribute("SYN")
//    vc.getAttribute("U3")
//    vc.getAttribute("U5")
//    vc.getAttribute("ASS")
//    vc.getAttribute("DSS")
//    vc.getAttribute("INT")
//    vc.getAttribute("R3")
//    vc.getAttribute("R5")
//    vc.getAttribute("OTH")
//    vc.getAttribute("CFL")
//    vc.getAttribute("ASP")
//    vc.getAttribute("MUT")
//    vc.getAttribute("VLD")
//    vc.getAttribute("G5A")
//    vc.getAttribute("G5")
//    vc.getAttribute("HD")
//    vc.getAttribute("GNO")
//    vc.getAttribute("KGValidated")
//    vc.getAttribute("KGPhase1")
//    vc.getAttribute("KGPilot123")
//    vc.getAttribute("KGPROD")
//    vc.getAttribute("OTHERKG")
//    vc.getAttribute("PH3")
//    vc.getAttribute("CDA")
//    vc.getAttribute("LSD")
//    vc.getAttribute("MTP")
//    vc.getAttribute("OM")
//    vc.getAttribute("NOC")
//    vc.getAttribute("WTD")
//    vc.getAttribute("NOV")
//    vc.getAttribute("CAF")
//    vc.getAttribute("COMMON")
     .setHgvsC(vc.getAttributeAsString("CLNHGVS", ""))
//    vc.getAttribute("CLNALLE")
//    vc.getAttribute("CLNSRC")
//    vc.getAttribute("CLNORIGIN")
//    vc.getAttribute("CLNSRCID")
//    vc.getAttribute("CLNSIG")
//    vc.getAttribute("CLNDSDB")
//    vc.getAttribute("CLNDSDBID")
//    vc.getAttribute("CLNDBN")
//    vc.getAttribute("CLNACC")
      .build()

    clinvar

  }
}

object ADAMOMIMAnnotation
{

  def apply(vc : VariantContext) : ADAMOMIMVariantAnnotation  =
  {
    val omim = ADAMOMIMVariantAnnotation.newBuilder
      .setOmimId(vc.getAttribute("CS").toString)
      .build()

    omim

  }
}



object ADAMSnpEffAnnotation
{

  def apply(vc : VariantContext) : ADAMSnpEffAnnotation  =
  {
    val snpeff = ADAMSnpEffVariantAnnotation.newBuilder
      .setOmimId(vc.getAttribute("CS").toString)
      .build()

    snpeff

  }
}