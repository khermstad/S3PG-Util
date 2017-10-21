import awscala._
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import s3._
import com.typesafe.config.ConfigFactory;
import scala.slick.driver.PostgresDriver.simple._

import scala.collection.mutable.ListBuffer

object S3PGHelper {

  def main(args: Array[String]): Unit = {

    // load aws config
    val awsAccessKey = ConfigFactory.load().getString("awsAccessKey")
    val awsSecretKey = ConfigFactory.load().getString("awsSecretKey")
    val s3Bucket = ConfigFactory.load().getString("s3Bucket")

    val awsCreds = new BasicAWSCredentials(awsAccessKey, awsSecretKey)
    val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard().withRegion("us-east-1").withCredentials(
      new AWSStaticCredentialsProvider(awsCreds)).build

    // get bucket
    var bucketList = s3Client.listObjects(s3Bucket)
    var objectSummaries = bucketList.getObjectSummaries
    while (bucketList.isTruncated()) {
      bucketList = s3Client.listNextBatchOfObjects(bucketList)
      objectSummaries.addAll(bucketList.getObjectSummaries)
    }

    var keys = ListBuffer[String]()

    // get all keys
    for (i <- 0 until objectSummaries.size()) {
      keys += objectSummaries.get(i).getKey

    }

    // returns true if key ends in .mp3
    def isMp3(key: String): Boolean = {
      if (key.length < 5) return false
      if (key.substring(key.length - 4, key.length) == ".mp3") {
        return true
      }
      false
    }

    // filtered to only keys that end with .mp3
    val keysFromS3 = keys.filter(isMp3);

    // now look for each key in db, if not there, delete it in s3
    val connectionUrl = ConfigFactory.load().getString("postgresUrl")

    // Tracks model for PG db
    class Tracks(tag: Tag) extends Table[(String, String, String, String, String,
      Int, Boolean, String, String, Int)](tag, "tracks") {

      def s3key = column[String]("s3key", O.PrimaryKey)
      def email = column[String]("email")
      def bucket = column[String]("bucket")
      def title = column[String]("title")
      def description = column[String]("description")
      def downloads = column[Int]("downloads")
      def approved = column[Boolean]("approved")
      def created = column[String]("created")
      def rejected = column[String]("rejected")
      def trackid = column[Int]("trackid")

      def * = (s3key, email, bucket, title, description, downloads, approved, created, rejected, trackid)
    }

    var keysFromPostgres = ListBuffer[String]();

    // db connection
    Database.forURL(connectionUrl, driver = "org.postgresql.Driver") withSession {
      implicit session =>
        val keys = TableQuery[Tracks]

        // iterate through tracks, push s3key to keysFromPostgres
        keys.list foreach { row =>
          keysFromPostgres += row._1
        }

    }

    // subtract keys from db with s3 - remove remainder from s3
    val keydiff = keysFromS3 -- keysFromPostgres

    // iterate through keys and remove from s3
    for (i <- 0 until keydiff.size) {
      s3Client.deleteObject(s3Bucket, keydiff(i))
    }
  }
}


