/*
 * Copyright (c) 2013. The Broad Institute
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
package edu.berkeley.cs.amplab.adam.rdd.recalibration

import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord._
import edu.berkeley.cs.amplab.adam.models.SnpTable
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord

object ReadCovariates {
  def apply(rec: RichADAMRecord, qualRG: QualByRG, covars: List[StandardCovariate],
            dbsnp: SnpTable = SnpTable()): ReadCovariates = {
    new ReadCovariates(rec, qualRG, covars, dbsnp)
  }
}

class ReadCovariates(val read: ADAMRecord, qualByRG: QualByRG, covars: List[StandardCovariate],
                     val dbSNP: SnpTable, val minQuality:Int = 2)
                    extends RichADAMRecord(read) with Iterator[BaseCovariates] with Serializable {

  def isLowQualityBase(qual : Byte) : Boolean = {
    qual.toInt <=  minQuality
  }

  def QualByRG(start: Int, end: Int): Array[Int] = {
    val rg_offset = RecalUtil.Constants.MAX_REASONABLE_QSCORE * read.getRecordGroupId
    qualityScores.slice(start, end).map(_.toInt + rg_offset)
  }

  val qualityStartOffset = qualityScores.takeWhile(isLowQualityBase).size
  val qualityEndOffset = qualityScores.size - qualityScores.reverseIterator.takeWhile(isLowQualityBase).size


  lazy val qualCovar: Array[Int] = QualByRG(qualityStartOffset, qualityEndOffset)
  lazy val requestedCovars: List[Array[Int]] = covars.map(covar => covar(this, qualityStartOffset, qualityEndOffset))

  var readOffset = qualityStartOffset

  lazy val masked = Range(qualityStartOffset, qualityEndOffset).map( offset => dbSNP.isMaskedAtReadOffset(read, offset))


  override def hasNext: Boolean = readOffset < qualityEndOffset

  override def next(): BaseCovariates = {
    val baseCovarOffset = readOffset - qualityStartOffset
    val mismatch = isMismatchAtReadOffset(readOffset)
    // FIXME: why does empty mismatch mean it should be masked?
    val isMasked = masked(baseCovarOffset) || mismatch.isEmpty
    // getOrElse because reads without an MD tag can appear during *application* of recal table
    val isMismatch = mismatch.getOrElse(false)
    val qualityScore = qualityScores(readOffset)
    readOffset += 1
    new BaseCovariates(qualCovar(baseCovarOffset), requestedCovars.map(v => v(baseCovarOffset)).toArray,
      qualityScore, isMismatch, isMasked)
  }

}

class BaseCovariates(val qualByRG: Int, val covar: Array[Int], val qual: Byte, val isMismatch: Boolean, val isMasked: Boolean) {}

// holder class
