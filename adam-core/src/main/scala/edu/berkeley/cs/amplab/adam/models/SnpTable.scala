package edu.berkeley.cs.amplab.adam.models

import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.avro.{ADAMVariant, ADAMRecord}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.collection.immutable._
import scala.collection.mutable
import java.io.File

class SnpTable(private val table: Map[String, Set[Long]]) extends Serializable with Logging {
  log.info("SNP table has %s contigs and %s entries".format(table.size, table.values.map(_.size).sum))

  def isMaskedAtReadOffset(read: ADAMRecord, offset: Int): Boolean = {
    val position = read.readOffsetToReferencePosition(offset)
    try {
      position.isEmpty || table(read.getReferenceName.toString).contains(position.get)
    } catch {
      case e: java.util.NoSuchElementException =>
        false
    }
  }
}

object SnpTable {
  def apply(): SnpTable = {
    new SnpTable(Map[String, Set[Long]]())
  }

  // `dbSNP` is expected to be a sites-only VCF
  def apply(dbSNP: File): SnpTable = {
    // parse into tuples of (contig, position)
    val lines = scala.io.Source.fromFile(dbSNP).getLines()
    val tuples = lines.filter(line => !line.startsWith("#")).map(line => {
      val split = line.split("\t")
      val contig = split(0)
      val pos = split(1).toLong
      (contig, pos)
    })
    // construct map from contig to set of positions
    // this is done in-place to reduce overhead
    val table = new mutable.HashMap[String, mutable.HashSet[Long]]
    tuples.foreach(tup => table.getOrElseUpdate(tup._1, { new mutable.HashSet[Long] }) += tup._2)
    // construct SnpTable from immutable copy of `table`
    createSnpTableFromMap(table)
  }

  def apply(vc : RDD[ADAMVariantContext]): SnpTable =
  {
    val variants = vc.flatMap(_.variants)
    val table = new mutable.HashMap[String, mutable.HashSet[Long]]
    val snpReferencePos = variants.map(variant => (variant.getReferenceName, variantPosToReference(variant))).collect()
    def addPosToSnpTable(contig:String, pos : Long) = {
      table.getOrElseUpdate(contig, { new mutable.HashSet[Long]}) += pos
    }
    snpReferencePos.foreach(tup => tup._2.foreach(pos => addPosToSnpTable(tup._1, pos)))
    createSnpTableFromMap(table)
  }

  def createSnpTableFromMap( table :mutable.HashMap[String, mutable.HashSet[Long]]) : SnpTable =
  {
    new SnpTable(table.mapValues(_.toSet).toMap)
  }

  def variantPosToReference( variant : ADAMVariant) : Seq[Long] =
  {
    if (variant.getPosition != null && variant.getVariant != null)  {
      variant.getReferenceAllele.zipWithIndex.map(allele => variant.getPosition + allele._2)
    }
    else
    {
      Seq.empty
    }
  }

}