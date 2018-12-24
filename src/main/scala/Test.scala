import org.joda.time.LocalDate

/**
  * Created by LZhan 
  * Time:8:49
  * Date:2018/11/30
  */
object Test {


  def main(args: Array[String]): Unit = {
    println("Hello,Scala")


   val date=LocalDate.now()
    val time=date.toDate.getTime
    val time1=time+24*60*60*1000
    print(new LocalDate(time1))


  }


}
