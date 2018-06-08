package com.talkingdata.addmp.tools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.Reader
import org.apache.hadoop.io.compress.CompressionCodecFactory

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * @author
 *
 */
object Hdfs {

  var configuration = new Configuration
  def fsc(conf:Configuration = configuration) = FileSystem.get( conf )
  def fs = fsc()
  def files(in:Path):Array[Path] = for( s <- fs.listStatus( in ) if s.getPath.getName != "_SUCCESS" && s.getPath.getName != "_temporary") yield s.getPath()
  def files(in:String):Array[Path] = files(new Path( in ))
  def exists( path : String ) = fs.exists( new Path(path) ) && !fs.exists(new Path(path,"_temporary"))
  def open(path:String):FSDataInputStream = open( new Path( path ) )
  def open(path:Path):FSDataInputStream = fs.open( path )
  def create(path:Path):FSDataOutputStream = fs.create( path )
  def create(path:String):FSDataOutputStream = create( new Path( path ) )
  def delete(path: Path, recursive: Boolean = false) = {
    fs.delete(path, recursive)
  }
  def size(path:String) = fs.getFileStatus( new Path( path ) ).getLen
  def lines(path:String) = {
    println("read ............")
    val in = open(path)
    val codec = new CompressionCodecFactory(Hdfs.fs.getConf).getCodec( new Path(path) )
    val cin = if( codec == null ) in else codec.createInputStream(in)
    val buffer = new ArrayBuffer[String]()
    buffer ++= Source.fromInputStream( cin , "UTF-8").getLines
    cin.close
    buffer
  }
  def text(path:String) = lines(path).mkString("\n")

  def text(path:String, f : String => Unit ) = {
    println("read ............")
    val in = open(path)
    val codec = new CompressionCodecFactory(Hdfs.fs.getConf).getCodec( new Path(path) )
    val cin = if( codec == null ) in else codec.createInputStream(in)
    Source.fromInputStream( cin , "UTF-8").getLines.foreach(f)
    cin.close
  }

  def  sequenceFileReader(in:Path) = {
    val _fs = fs
    val conf = _fs.getConf
    new Reader(fs , in , conf )
  }

  def summary(path:Path) = {
    val _fs = fs
    _fs.getContentSummary(path)
  }

  def length(path:Path) = summary(path).getLength

}