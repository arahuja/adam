/*
 * Copyright (c) 2013. Mount Sinai School of Medicine
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

package edu.berkeley.cs.amplab.adam.rdd.variation

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.models.{ConcordanceTable, ADAMVariantContext}
import edu.berkeley.cs.amplab.adam.avro.{ADAMGenotype, ADAMDatabaseVariantAnnotation}
import org.apache.spark.SparkContext._
import edu.berkeley.cs.amplab.adam.rich.{GenotypeType, RichADAMVariant}
import edu.berkeley.cs.amplab.adam.rich.RichADAMGenotype._

class ADAMVariantContextRDDFunctions(rdd: RDD[ADAMVariantContext]) extends Serializable with Logging {
  initLogging()

  /**
   * Left outer join database variant annotations
   *
   */
  def joinDatabaseVariantAnnotation(ann: RDD[ADAMDatabaseVariantAnnotation]): RDD[ADAMVariantContext] = {
    rdd.keyBy(_.variant)
      .leftOuterJoin(ann.keyBy(_.getVariant))
      .values
      .map { case (v:ADAMVariantContext, a) => new ADAMVariantContext(v.variant, v.genotypes, databases = a) }
  }

  def sampleNames(): RDD[String] = {
    rdd.flatMap(_.genotypes.map(_.getSampleId)).distinct.map(_.toString)
  }
}

class ADAMGenotypeRDDFunctions(rdd: RDD[ADAMGenotype]) extends Serializable with Logging {
  initLogging()

  def toADAMVariantContext(): RDD[ADAMVariantContext] = {
    rdd.keyBy({ g => RichADAMVariant.variantToRichVariant(g.getVariant) })
      .groupByKey
      .map { case (v:RichADAMVariant, g) => new ADAMVariantContext(v, genotypes = g) }
  }

  def concordanceWith(truth: RDD[ADAMGenotype]) : RDD[(String, ConcordanceTable)] = {
    val keyedTest  =   rdd.keyBy(g => (g.getVariant, g.getSampleId.toString) : (RichADAMVariant, String))
    val keyedTruth = truth.keyBy(g => (g.getVariant, g.getSampleId.toString) : (RichADAMVariant, String))

    val inTest = keyedTest.leftOuterJoin(keyedTruth)
    val justInTruth = keyedTruth.subtractByKey(inTest)

    // Compute RDD[sample -> ConcordanceTable] across variants/samples
    val bySample = inTest.map({
      case ((_, sample), (l, Some(r))) => sample -> (l.getType, r.getType)
      case ((_, sample), (l, None))    => sample -> (l.getType, GenotypeType.NO_CALL)
    }).union(justInTruth.map({ // add in "truth-only" entries
      case ((_, sample), r) => sample -> (GenotypeType.NO_CALL, r.getType)
    })).combineByKey(
      ConcordanceTable.create,
      ConcordanceTable.addComparison,
      ConcordanceTable.mergeTable
    )

    bySample
  }
}
