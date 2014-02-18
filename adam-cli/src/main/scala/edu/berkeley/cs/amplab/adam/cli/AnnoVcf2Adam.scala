/*
* Copyright (c) 2014. Mount Sinai School of Medicine
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

import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.util.{SnpEffEffect}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{Logging, SparkContext}
import org.kohsuke.args4j.Argument
import edu.berkeley.cs.amplab.adam.converters.{VariantAttributeConverter, VariantContextConverter}
import fi.tkk.ics.hadoop.bam.{VariantContextWritable, VCFInputFormat}
import org.apache.hadoop.io.LongWritable
import parquet.hadoop.util.ContextUtil
import org.broadinstitute.variant.variantcontext.VariantContext
import edu.berkeley.cs.amplab.adam.avro._
import org.broadinstitute.variant.vcf.{VCFHeaderLineType, VCFInfoHeaderLine}
import edu.berkeley.cs.amplab.adam.converters.VariantAttributeConverter.AttrKey
import edu.berkeley.cs.amplab.adam.converters.VariantAttributeConverter.AttrKey

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

  private val CLINVAR_KEYS: Seq[AttrKey] = Seq(
    AttrKey("rsDbsnp", VariantAttributeConverter.attrAsInt _, new VCFInfoHeaderLine("dbSNP ID", 1, VCFHeaderLineType.Integer, "dbSNP ID")),
    AttrKey("hgvs", VariantAttributeConverter.attrAsString _, new VCFInfoHeaderLine("CLNHGVS", 1, VCFHeaderLineType.String, "Variant names from HGVS. The order of these variants corresponds to the order of the info in the other clinical  INFO tags.")),
    AttrKey("clinicalSignificance", VariantAttributeConverter.attrAsString _, new VCFInfoHeaderLine("CLNSIG", 1, VCFHeaderLineType.String, "Variant Clinical Significance, 0 - unknown, 1 - untested, 2 - non-pathogenic, 3 - probable-non-pathogenic, 4 - probable-pathogenic, 5 - pathogenic, 6 - drug-response, 7 - histocompatibility, 255 - other")),
    AttrKey("geneSymbol", VariantAttributeConverter.attrAsString _, new VCFInfoHeaderLine("GENEINFO", 1, VCFHeaderLineType.String, "Pairs each of gene symbol:gene id.  The gene symbol and id are delimited by a colon (:) and each pair is delimited by a vertical bar"))
 )

  private lazy val headerLines : Map[String,(Int,Object => Object)] = CLINVAR_KEYS.map(field => {
    var avroField = ADAMClinvarVariantAnnotation.getClassSchema.getField(field.adamKey)
    field.vcfKey -> (avroField.pos, field.attrConverter)})(collection.breakOut)

  def apply(vc : VariantContext) : ADAMClinvarVariantAnnotation  =
  {
    val clinvar = ADAMClinvarVariantAnnotation.newBuilder
      .build()
    VariantAttributeConverter(clinvar, vc, headerLines).asInstanceOf[ADAMClinvarVariantAnnotation]
  }
}

object ADAMOMIMAnnotation
{
  private val OMIM_KEYS: Seq[AttrKey] = Seq(
    AttrKey("omimId", VariantAttributeConverter.attrAsString _, new VCFInfoHeaderLine("VAR", 1, VCFHeaderLineType.String, "MIM entry with variant mapped to rsID"))
  )

  private lazy val headerLines : Map[String,(Int,Object => Object)] = OMIM_KEYS.map(field => {
    var avroField = ADAMOMIMVariantAnnotation.getClassSchema.getField(field.adamKey)
    field.vcfKey -> (avroField.pos, field.attrConverter)})(collection.breakOut)

  def apply(vc : VariantContext) : ADAMOMIMVariantAnnotation  =
  {
    val omim = ADAMOMIMVariantAnnotation.newBuilder
      .build()
    VariantAttributeConverter(omim, vc, headerLines).asInstanceOf[ADAMOMIMVariantAnnotation]
  }
}



object ADAMSnpEffAnnotation
{

  def apply(vc : VariantContext) : ADAMSnpEffVariantAnnotation  =
  {
    val snpeff = SnpEffEffect(vc.getAttributeAsString(SnpEffEffect.SNP_EFF_INFO_FIELD, ""))
    snpeff
  }
}