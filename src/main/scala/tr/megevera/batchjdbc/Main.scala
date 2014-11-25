package tr.megevera.batchjdbc

import java.sql.{DriverManager, Connection}
import java.util.UUID
import java.util.concurrent.Executors

import com.typesafe.config.ConfigFactory

/**
 * Created by alaym on 25.11.2014.
 */
object Main {

  Class.forName("com.mysql.jdbc.Driver")

  private val config = ConfigFactory.load()
  private val url = config.getString("db.url")
  private val user = config.getString("db.user")
  private val password = config.getString("db.password")
  private val size = config.getInt("size")
  private val oneByOneProgress = config.getInt("onebyone.progress")

  def conn(url: String, user: String, password: String): Connection = {
    DriverManager.getConnection(url, user, password)
  }

  def withConn(rewriteBatched: Boolean)(block: Connection => Unit): Unit = {

    val c =
      if(rewriteBatched)
        conn(s"$url&rewriteBatchedStatements=true", user, password)
      else
        conn(s"$url&rewriteBatchedStatements=false", user, password)

    scala.util.control.Exception.allCatch.andFinally(c.close())(block(c))


  }

  case class User(name: String, surname: String)

  def createNUsers(n: Int): List[User] = (0 until n) map (_ => User(UUID.randomUUID().toString, UUID.randomUUID().toString)) toList

  def timeit[T](block: => T): (Long, T) = {
    val start = System.currentTimeMillis()
    val result = block
    val duration = System.currentTimeMillis() - start
    (duration, result)
  }

  def insertOneByOne(users: List[User]): Unit = withConn(false) { connection =>

    val pst = connection.prepareStatement("insert into users(name, surname) values(?,?)")

    val durations = users.zipWithIndex.map { case (user, index) =>

      pst.setString(1, user.name)
      pst.setString(2, user.surname)

      val (duration, _) = timeit(pst.execute())

      if ((index + 1) % oneByOneProgress == 0) {
        println(s"one by one: inserted ${index + 1}")
      }

      duration

    }

    val sum = durations.sum
    val avg = sum.toDouble / users.size.toDouble

    println(s"one by one insert, avg: $avg ms, sum: $sum ms")


  }

  def insertBatch(users: List[User]): Unit = withConn(false) { connection =>

    val initialBatch = connection.prepareStatement("insert into users(name, surname) values(?,?)")

    val finalBatch = (initialBatch /: users) { (accBatch, user) =>

      accBatch.setString(1, user.name)
      accBatch.setString(2, user.surname)
      accBatch.addBatch()

      accBatch

    }

    val (duration, _) = timeit(finalBatch.executeBatch())

    println(s"batch insert took: $duration ms")

  }

  def insertBatchWithRewrite(users: List[User]): Unit = withConn(true) { connection =>

    val initialBatch = connection.prepareStatement("insert into users(name, surname) values(?,?)")

    val finalBatch = (initialBatch /: users) { (accBatch, user) =>

      accBatch.setString(1, user.name)
      accBatch.setString(2, user.surname)
      accBatch.addBatch()

      accBatch

    }

    val (duration, _) = timeit(finalBatch.executeBatch())

    println(s"batch insert with rewrite took: $duration ms")

  }

  def main(args: Array[String]): Unit = {

    val users = createNUsers(size)
    println(s"created $size users")

    insertOneByOne(users)
    insertBatch(users)
    insertBatchWithRewrite(users)



  }

}
