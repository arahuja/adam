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

package edu.berkeley.cs.amplab.adam.util

import edu.berkeley.cs.amplab.adam.avro.ADAMSnpEffVariantAnnotation

object SnpEffEffect {

  val SNP_EFF_INFO_FIELD = "EFF"
  val EFFECT_PATTERN =  new scala.util.matching.Regex("(\\w+)\\((.+)\\)")
  val EFFECT_FIELDS =  new scala.util.matching.Regex("\\w+\\((.+)\\)")

  def apply ( snpEffInfo : String ) : ADAMSnpEffVariantAnnotation =
  {
    parse(snpEffInfo)
  }


  def parse ( snpEffFields : String ) : ADAMSnpEffVariantAnnotation =
  {

    val EFFECT_PATTERN(effectType, effectProp)  = snpEffFields
    val effectFields = effectProp.split("\\|")
    val effectImpact = effectFields(0)

    val effect = ADAMSnpEffVariantAnnotation.newBuilder
      .setEffect(effectType)
      .setImpact(effectImpact)
      .setFunctionalClass(effectFields(1))
      .setCodonChange(effectFields(2))
      .setAminoAcidChange(effectFields(3))
      .setGeneName(effectFields(4))
      .setCoding(effectFields(5))
      .setGeneBiotype(effectFields(6))
      .setTranscriptID(effectFields(9))

      .build
    effect

  }

  def main(args: Array[String]) {
    val testLine = "FRAME_SHIFT(HIGH||-|-221|495|FTCD|protein_coding|CODING|ENST00000355384|6|1)"
    val effect = SnpEffEffect(testLine)
  }
}



//class SnpEffEffect(val effect : String, val impact : String)
//{
//
//}
