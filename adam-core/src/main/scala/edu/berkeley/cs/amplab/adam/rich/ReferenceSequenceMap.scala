package edu.berkeley.cs.amplab.adam.rich


import net.sf.picard.reference.{IndexedFastaSequenceFile, ReferenceSequence, ReferenceSequenceFile}
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import java.io.File

object ReferenceSequenceMap {

  def apply(referenceReader : ReferenceSequenceFile) : ReferenceSequenceMap = {
      new ReferenceSequenceMap(referenceReader)
  }
}

class ReferenceSequenceMap(val referenceFile : ReferenceSequenceFile) extends Serializable {

  private val referenceMap = ReferenceSequenceIterator(referenceFile).map( reference => (reference.getName, reference )).toMap


  def getReferenceSubSequence(read : RichADAMRecord) : Option[ReferenceSequence] = {
    referenceMap.get(read.getReferenceName.toString)
  }

}

object ReferenceSequenceIterator {

  def apply(referenceFile : ReferenceSequenceFile): ReferenceSequenceIterator =
  {
    new ReferenceSequenceIterator(referenceFile)
  }
}


class ReferenceSequenceIterator(referenceSequenceFile : ReferenceSequenceFile) extends Iterator[ReferenceSequence]
{

  var nextSequence = referenceSequenceFile.nextSequence()
  def hasNext: Boolean = nextSequence != null
  def next() : ReferenceSequence = {
    val lastSequence = nextSequence
    nextSequence = referenceSequenceFile.nextSequence()
    lastSequence
  }
}
