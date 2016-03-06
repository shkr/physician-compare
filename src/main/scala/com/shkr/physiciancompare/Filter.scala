package com.shkr.physiciancompare

import org.apache.spark.rdd.RDD

/**
 * Created by shashank on 3/1/16.
 */
object Filter {

  object Physician {

    val credential: Seq[String] = Seq("MD", "DO")

    val notSpeciality: Seq[String] = Seq(
      "Audiologist",
      "Midwife",
      "Optometry",
      "Philosophy",
      "Worker",
      "Practitioner",
      "Assistant",
      "Therapist",
      "Nurse",
      "Nutritionist"
    ).map(_.toUpperCase)
  }

  def physicianFilter(header: Seq[String], input: RDD[Seq[String]]): (Seq[String], RDD[Seq[String]]) = {
    val headerWithIndex: Map[String, Int] = header.zipWithIndex.toMap
    (header, input.filter(item => {
      val credential: String = item(headerWithIndex("Credential"))
      val primarySpeciality: String = item(headerWithIndex("Primary specialty"))

      (credential.isEmpty || Physician.credential.contains(credential)) && (
        primarySpeciality.isEmpty || !Physician.notSpeciality.exists(primarySpeciality.contains)
      )
    }))
  }

  def columnFilter(header: Seq[String], selectColumns: Seq[String], input: RDD[Seq[String]]): (Seq[String], RDD[Seq[String]]) = {
    val headerWithIndex: Map[String, Int] = header.zipWithIndex.toMap
    (selectColumns, input.map(record => selectColumns.map(key => record(headerWithIndex(key)))))
  }
}
