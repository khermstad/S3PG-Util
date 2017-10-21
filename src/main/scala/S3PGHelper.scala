import awscala._
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import s3._
import com.typesafe.config.ConfigFactory;

import scala.collection.mutable.ListBuffer

object S3PGHelper {
  def main(args: Array[String]): Unit = {

    val awsAccessKey = ConfigFactory.load().getString("awsAccessKey")
    val awsSecretKey = ConfigFactory.load().getString("awsSecretKey")
    val s3Bucket = ConfigFactory.load().getString("s3Bucket")

    val awsCreds = new BasicAWSCredentials(awsAccessKey, awsSecretKey)
    val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard.withCredentials(new AWSStaticCredentialsProvider(awsCreds)).build


    implicit val s3 = S3.at(Region.US_EAST_1)

    var bucketList = s3Client.listObjects(s3Bucket)
    var objectSummaries = bucketList.getObjectSummaries
    while (bucketList.isTruncated()) {
      bucketList = s3.listNextBatchOfObjects(bucketList)
      objectSummaries.addAll(bucketList.getObjectSummaries)
    }

    var keys = ListBuffer[String]()

    for (i <- 0 until objectSummaries.size()) {
      keys += objectSummaries.get(i).getKey

    }

    def isMp3(key: String): Boolean = {
      if (key.length < 5) return false
      if (key.substring(key.length - 4, key.length) == ".mp3") {
        return true
      }
      false
    }

    // filtered to only keys that end with .mp3
    val mp3keys = keys.filter(isMp3);

    println(mp3keys)
  }
}


