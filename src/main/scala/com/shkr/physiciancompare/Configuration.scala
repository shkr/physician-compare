package com.shkr.physiciancompare

/**
 * Created by shashank on 3/3/16.
 */
object Configuration {

  //Spark
  val APP_NAME: String = "medicare_physician"
  val NO_OF_CORES: String = "5"

  //Dataset
  val SOURCE_URL: String = "https://data.medicare.gov/api/views/s63f-csi6/rows.csv?accessType=DOWNLOAD"
  val WORK_DIRECTORY: String = "data/"
  val SOURCE_FILE: String = WORK_DIRECTORY + "National_Downloadable_File.csv"
  val SAVE_CLEAN_FILE_TO: String = WORK_DIRECTORY + "clean"

  val primaryKey: Seq[String] = Seq(
    "NPI",
    "Professional Enrollment ID",
    "Group Practice PAC ID",
    "Line 1 Street Address", "Line 2 Street Address", "City", "State", "Zip Code"
  )

  val keptKeys: Seq[String] = Seq(
    "NPI",
    "PAC ID",
    "Professional Enrollment ID",
    "Gender",
    "Medical school name",
    "Graduation year",
    "Primary specialty",
    "Organization legal name",
    "Group Practice PAC ID",
    "Number of Group Practice members",
    "Line 1 Street Address", "City", "State", "Zip Code",
    "Claims based hospital affiliation CCN 1",
    "Claims based hospital affiliation LBN 1",
    "Claims based hospital affiliation CCN 2",
    "Claims based hospital affiliation LBN 2",
    "Claims based hospital affiliation CCN 3",
    "Claims based hospital affiliation LBN 3",
    "Claims based hospital affiliation CCN 4",
    "Claims based hospital affiliation LBN 4",
    "Claims based hospital affiliation CCN 5",
    "Claims based hospital affiliation LBN 5"
  )

  //Column Stats
  val COLUMN_STATS_LIMIT: Int = 10

  //GraphDefinition
  trait GraphDefinition extends Serializable {
    val LEFT_VERTEX_ID: String
    val LEFT_PROPERTIES: Seq[String]
    val RIGHT_VERTEX_ID: String
    val RIGHT_PROPERTIES: Seq[String]
    val EDGE_PROPERTIES: Seq[String]
  }

  //Bipartite Graph
  object GroupPracticeGraph extends GraphDefinition {

    val LEFT_VERTEX_ID: String = "NPI"
    val LEFT_PROPERTIES: Seq[String] = Seq("Gender", "Medical school name", "Graduation year", "Primary specialty")
    val RIGHT_VERTEX_ID: String = "Group Practice PAC ID"
    val RIGHT_PROPERTIES: Seq[String] = Seq(
      "Organization legal name",
      "Number of Group Practice members",
      "Claims based hospital affiliation CCN 1",
      "City",
      "State"
    )
    val EDGE_PROPERTIES: Seq[String] = Seq("Primary specialty")
  }

  object CCNGraph extends GraphDefinition {

    val LEFT_VERTEX_ID: String = "NPI"
    val LEFT_PROPERTIES: Seq[String] = Seq("Gender", "Medical school name", "Graduation year", "Primary specialty")
    val RIGHT_VERTEX_ID: String = "Claims based hospital affiliation CCN 1"
    val RIGHT_PROPERTIES: Seq[String] = Seq(
      "Claims based hospital affiliation CCN 2",
      "Claims based hospital affiliation CCN 3",
      "Claims based hospital affiliation CCN 4",
      "Claims based hospital affiliation CCN 5",
      "Group Practice PAC ID",
      "Organization legal name",
      "City",
      "State"
    )
    val EDGE_PROPERTIES: Seq[String] = Seq("Primary specialty")
  }

  //Network Stats
  val PAGE_RANK_LIMIT: Int = 5
  val CONNECTED_COMPONENTS_LIMIT: Int = 5
  val PAGE_RANK_TOLERANCE: Double = 0.001

  //OUTPUT
  val COLUMN_STATS_OUTPUT: String = WORK_DIRECTORY + "column_stats.json"
  val PAGE_RANK_OUTPUT = WORK_DIRECTORY + "page_rank.json"
  val CONNECTED_COMPONENTS_OUTPUT = WORK_DIRECTORY + "connected_components.json"
}
