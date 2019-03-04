package com.github.heuermh.nameless.ds

/**
 * Decorates a nameless dataset with metadata.
 *
 * An example metadata trait:
 * <code>
 * trait WithReferences[T, U <: Product, V <: NamelessDataset[T, U, V]] extends WithMetadata {
 *   val references: ReferenceSet
 *
 *   override protected def saveMetadata(pathName: String): Unit = {
 *     saveReferences(pathName)
 *   }
 *
 *   def replaceReferences(references: ReferenceSet): V
 *
 *   protected def saveReferences(pathName: String): Unit
 * }
 * </code>
 */
trait WithMetadata {

  /**
   * Save these metadata to the specified path name.
   *
   * @param pathName path name to save these metadata to
   */
  protected def saveMetadata(pathName: String): Unit
}
