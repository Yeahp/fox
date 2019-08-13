package io

import java.io.{FileWriter, PrintWriter}

object ScalaReadFile {
  def main(args: Array[String]): Unit = {

    ///////////////////////////////////////////////////
    //  EXAMPLE ONE: read data from various sources  //
    ///////////////////////////////////////////////////
    /*
    // 1 --  read file and print it
    val file1 = Source.fromFile("C:/Users/erqi/Desktop/sql_unification.txt")
    val lines = file1.getLines().toArray
    for(line <- lines){
      line.trim.split(" ").foreach(println)
    }
    file1.close()

    // 2 -- from console
    val digit = readInt()
    println(digit)

    // 3 -- from URL
    val urlFile = Source.fromURL("http://www.baidu.com")
    for(line <- urlFile.getLines()){
      println(line)
    }

    // 4 -- from string
    val s = Source.fromString("we are the world!")
    for(line <- s.getLines()){
      println(line)
    }

    // 5 -- from stdin
    val s = Source.stdin
    for(line <- s.getLines()){
      println(line)
    }

    // 6 -- read binary file
    val file = new File("path")
    val in = new FileInputStream(file)
    val bytes = new Array[Byte](file.length.toInt)
    in.read(bytes)
    in.close()
    */

    ///////////////////////////////////////////////////
    ///////  EXAMPLE TWO: write data into file  ///////
    ///////////////////////////////////////////////////
    val out = new FileWriter("C:/Users/erqi/Desktop/we.txt")
    for(i <- 1 to 100){
      out.write(i.toString)
    }
    out.close()

    val out_ = new PrintWriter("C:/Users/erqi/Desktop/wee.txt")
    for(i <- 0 to 99){
      out_.println(i.toString)
    }
    out_.close()

    /********************  NOTE  *********************/
    // if we want to append file
    val out__ = new FileWriter("C:/Users/erqi/Desktop/we.txt", true)
    // if we want to write a number a line in Windows system
    // for FileWriter, we have
    out__.write("we" + "\r\n")
    // for PrintWriter, we have two ways
//    val out___ = new PrintWriter("C:/Users/erqi/Desktop/we.txt", true)
//    out___.println("we")
//    out___.print("we"+"\r\n")

    ///////////////////////////////////////////////////
    //////  EXAMPLE THREE: go through directory  //////
    ///////////////////////////////////////////////////
    /*
    // find all the directories
    def subdirs1(dir:File):Iterator[File] = {
      val children = dir.listFiles.filter(_.isDirectory())
      children.toIterator ++ children.toIterator.flatMap(subdirs1 _)
    }

    // find all the files
    def subdirs2(dir: File): Iterator[File] = {
      val d = dir.listFiles.filter(_.isDirectory)
      val f = dir.listFiles.filter(_.isFile).toIterator
      f ++ d.toIterator.flatMap(subdirs2 _)
    }

    // find all the files and directories
    def subdirs3(dir: File): Iterator[File] = {
      val d = dir.listFiles.filter(_.isDirectory)
      val f = dir.listFiles.toIterator
      f ++ d.toIterator.flatMap(subdirs3 _)
    }

    for(d <- subdirs1(new File("C:/Users/erqi/Desktop/优秀研究生电子"))){
      println(d)
    }

    for(d <- subdirs2(new File("C:/Users/erqi/Desktop/优秀研究生电子"))){
      println(d)
    }

    for(d <- subdirs3(new File("C:/Users/erqi/Desktop/优秀研究生电子"))){
      println(d)
    }
    */
  }
}
