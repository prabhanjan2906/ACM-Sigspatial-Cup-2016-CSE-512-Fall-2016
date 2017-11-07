//http://www.nodalpoint.com/spark-dataframes-from-csv-files/

import scala.collection.mutable.ListBuffer
import Math.sqrt;

val input = sc.textFile ("/tmp/NYC_yellow.csv") 
val numOfDays = 31
val NY_Min_X_CoOrd = 40.5
val NY_Max_X_CoOrd = 40.9
val NY_Min_Y_CoOrd = -74.25 
val NY_Max_Y_CoOrd = -73.7 
val NY_Min_X = NY_Min_X_CoOrd / 0.01 
val NY_Max_X = NY_Max_X_CoOrd / 0.01 
val NY_Min_Y = NY_Min_Y_CoOrd / 0.01 
val NY_Max_Y = NY_Max_Y_CoOrd / 0.01 
val numOfX = (NY_Max_X - NY_Min_X)	//lat
val numOfY = (NY_Max_Y - NY_Min_Y)	//long
val N = numOfX * numOfY * numOfDays

def mapEachRowToArray (e: String):((Int, Int, Int), Int) = {
  val temp = e.split (",")
    val j = (temp (5).trim ().toDouble / 0.01).toInt
    val i = (temp (6).trim ().toDouble / 0.01).toInt
    if (i >= NY_Min_X && i <= NY_Max_X && j >= NY_Min_Y && j <= NY_Max_Y)
    {
     val date = temp (1).slice (0, temp (1) indexOf ' ')	//get the date part
     val day = date.split ('-') (2).trim ().toInt
     return ((i.toInt, j.toInt, day), 1)}
  else
    {
     return ((0, 0, 0), 0)}
}


val header = input.first()

val dataMatrix = input.filter (row =>row != header).map(mapEachRowToArray _).reduceByKey (_ + _).filter { case (t: (Int, Int, Int), s:Int) => t._1 != 0 }
val Xlist = dataMatrix.values.collect();
val valuesOfTheCube = sc.broadcast(dataMatrix.collect()).value;
//.collect()

/* continuing here... */
val parallelized = sc.parallelize(Xlist)
val sigma_x = parallelized.sum()
val mean = sigma_x/N


//calculate standard deviation
val squaredX = parallelized.map{ (x) => x*x}
val div = squaredX.sum()/N
val diff = div - (mean * mean)
val standardDeviation = Math.sqrt(diff)

def getNeighborsForACell(aCell: (Int, Int, Int)): List[(Int, Int, Int)]  = {
val neighbors: ListBuffer[(Int, Int, Int)] = new ListBuffer[(Int, Int, Int)]();
for (i <- -1 to 1){
for (j <- -1 to 1) {
for (k <- -1 to 1) {
neighbors += ((aCell._1 +i, aCell._2 + j, aCell._3 + k))
}
}
}
println(neighbors);
return neighbors.toList;
}

def getSigmaList(valuesOfTheCube: Array[((Int, Int, Int), Int)], neighbors: List[(Int, Int, Int)], theCell: (Int, Int, Int)): List[Int] = {
      val sigmaList: ListBuffer[Int] = new ListBuffer[Int]();for(n <- neighbors) {
      for(v <- valuesOfTheCube) {
      val aCell = v._1
      if( n == aCell ) {
      sigmaList += v._2
      }}}
      return sigmaList.toList;
}

def getSumNeighbors(sigmaList: List[Int]): Double = {
      var sum: Double = 0;
      for (aNeighbor <- sigmaList) {
      sum = sum + aNeighbor;
      }
      return sum;
}

def getDenominator(neighborCount: Int): Double = {
      val x = N * neighborCount;
      val y  = neighborCount * neighborCount;
      val diff = x-y;
      val div =diff/((N-1));
      val result = Math.sqrt(div);
      return result;
}


def calculateZscore(sigmaList: List[Int], mean: Double, sd: Double): Double = {
      val neighborCount: Int = sigmaList.length;
      val sigmaSum = getSumNeighbors(sigmaList);
      val numerator = sigmaSum - (mean * neighborCount);
      var denominator = getDenominator(neighborCount);
      denominator = sd * denominator;
      val zscore = numerator / denominator;
      return zscore;
}

def calculateGetis(aCell: ((Int, Int, Int), Int)): (Double, (Int, Int, Int)) = {
val cell = aCell._1
val count = aCell._2
val neighbors = getNeighborsForACell(cell);
val sigmaList = getSigmaList(valuesOfTheCube, neighbors, cell);
val zscore: Double = calculateZscore(sigmaList, mean, standardDeviation);
return (zscore, cell);
}

val finalResult = dataMatrix.map( calculateGetis _).takeOrdered(50);
