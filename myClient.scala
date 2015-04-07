import akka.actor._
import scala.util.Random
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Queue
import com.typesafe.config.ConfigFactory
import java.net.InetAddress
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

case class initializeUser()
case class sendRange(start:Int,end:Int,serverListSize:Int,numOfUsers:Int,serverList:List[ActorRef])
case class Hi(s:String)
case class serverInit(id:Int, numOfServers:Int, numOfUsersPerServer:Int,boss:ActorRef,serverActorList:List[ActorRef])
case class userTweet(id:Int,tweet:String)
case class myServer(id:Int,yourServer:Int,yourclient:ActorRef,serverActorList:List[ActorRef])
case class userList(users:Map[Int,Int])
case class startTweet()
case class fetchTweet(id:Int,list:List[Int])
case class yourNumOfFollowers(x:Int,total:Int)
case class sendMeTweets(x:Int,num:Int,userref:ActorRef)
case class tweetsOfFollower(tweetlist:ListBuffer[String])
case class followers(id:Int,list:List[Int],yourclient:ActorRef)
case class madeTheUsers()
case class doneTweeting(c:Int)
case class trackTime(b:Long,userc:Int,serverc:Int)
case class getTime(b:Long)
case class nums(numOfServers:Int,serverActorList:List[ActorRef],numOfUsers:Int)
case class sendInfo(numOfServers:Int,serverActorList:List[ActorRef],numOfUsers:Int)



object Main extends App{
  override def main(args: Array[String]):Unit= {
   
    val noOfUsers:Int = 150
    
    val hostIP = InetAddress.getLocalHost.getHostAddress()
	val RemoteIP:String=args(0)
	println("Remote="+RemoteIP)
	
    val config = ConfigFactory.parseString("""
    akka {
       actor {
           provider = "akka.remote.RemoteActorRefProvider"
             }
       remote {
           enabled-transports = ["akka.remote.netty.tcp"]
       netty.tcp {
           hostname = """ + hostIP + """
           port = 0
                 }
             }
        }
    """) 
   
    val system = ActorSystem("actorSystem", ConfigFactory.load(config))  
    
    val duration = Duration(100000, SECONDS)
    //val path: String =  "akka.tcp://Server@"+RemoteIP+":2552/user/boss"

    var future = system.actorSelection("akka.tcp://Server@"+RemoteIP+":2252/user/boss").resolveOne(duration)

    var client = system.actorOf(Props(new Clients(noOfUsers, future)), "client")
    //client ! "initiate"
  } 

}

case class Clients(noOfUsers:Int,future: scala.concurrent.Future[akka.actor.ActorRef]) extends Actor
{
  
  var bossserver : ActorRef = _ 
  
  
  future.onComplete {
     
      case Success(value) => bossserver = value
    		  				bossserver!"Connection Complete"
    		  				println("connection done")
        					 self ! "initiate"
    		  				 
     
      case Failure(e) => e.printStackTrace
  }
  
val usersystem = ActorSystem("User")
var id:Int=0
var startRange:Int=0
var endRange:Int=0
var serverActorList:List[ActorRef]=Nil
var serverListSize:Int=0
var userActorList:List[ActorRef] = Nil
var yourServer:Int=0
var numOfUsers:Int=0
var yourclient:ActorRef=null
var j:Int=0
var i:Int=0
var serUser:Map[Int,Int] = Map()

def createUsers(start:Int, end:Int,yourclient:ActorRef,serverListSize:Int,serverActorList:List[ActorRef])=
{
this.yourclient=yourclient
var range=(end-start)+1
j=start
while(j<=end)
{
userActorList ::= usersystem.actorOf(Props[Users])
j+=1
}
j=start
while(j<=end && i<=userActorList.length-1)
{
yourServer=j%serverListSize

userActorList(i)!myServer(j,yourServer,yourclient,serverActorList)
serUser += (j-> yourServer)
j+=1
i+=1
}

i=0
j=0
while(j<=serverActorList.length-1)
{
var users:Map[Int,Int]=Map()

serUser.keys.foreach{ i =>

if(serUser(i)==j)
{
users+=i->serUser(i)
}
}
serverActorList(j)!userList(users)
j+=1
}
i=0
j=0

while(i<=(end-start))
{
var x:Int=util.Random.nextInt(numOfUsers)
userActorList(i)!yourNumOfFollowers(x,numOfUsers)
i+=1
}
}

def receive ={
  // case initializeUser() =>	
    // bossserver ! "Connection complete"
  case "initiate"=> println("hi i am client")
  
/*case sendRange(startRange:Int,endRange:Int,serverListSize,numOfUsers,serverList:List[ActorRef])=>{
this.startRange=startRange
this.endRange=endRange
this.serverListSize=serverListSize
this.numOfUsers =numOfUsers
this.serverActorList=serverList
println("my user range is "+startRange+" to "+endRange)
createUsers(startRange,endRange,self,serverListSize,serverActorList)
} */
     
     case sendInfo(numOfServers:Int,serverActorList:List[ActorRef],numOfUsers:Int)=>
       {
         this.serverListSize=numOfServers
         this.serverActorList=serverActorList
         this.numOfUsers=numOfUsers
         startRange=0
         endRange=numOfUsers-1
         println("my user range is "+startRange+" to "+endRange)
         createUsers(startRange,endRange,self,serverListSize,serverActorList)
       }

case madeTheUsers()=>{

val startTimer: Long = System.currentTimeMillis
val y=util.Random.nextInt(10)+1
i=0
while(i<=endRange-startRange)
{
  val startTimer: Long = System.currentTimeMillis
userActorList(i)!startTweet()
i+=1
}
}
}

}
case class Users() extends Actor{
var id:Int=0
var yourServer:Int=0
var x:Int=0
var i:Int=0
var y:Int=0
var serverActor:ActorRef=null
var serverList:List[ActorRef]=null
var list:List[Int] = Nil
var yourclient:ActorRef=null
val flist = new ListBuffer[Int]()
var counts:Int=0
var tlist:ListBuffer[String]=null
var tweetc:Int=0

def bombardTweets(y: Int)={

for(a<-1 to y)
{
  tweetc+=1
counts+=1
var data=randomAlphaNumericString(140)
serverList(yourServer)!userTweet(id,data)
if(counts%5==0){
   
	serverList(yourServer)!fetchTweet(id,list)
}
}
serverList(yourServer)!doneTweeting(tweetc)
}

def randomAlphaNumericString(line: Int): String = {

val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
randomStringFromCharList(line, chars)
}

def randomStringFromCharList(line: Int, chars: Seq[Char]): String = {
val sb = new StringBuilder
for (i <- 1 to line) {
val randomNum = util.Random.nextInt(chars.length)
sb.append(chars(randomNum))
}
sb.toString
}

def receive={
case myServer(id:Int,yourServer:Int,yourclient:ActorRef,serverActorList:List[ActorRef])=>
{
this.id=id
this.yourServer=yourServer
this.serverList=serverActorList
this.yourclient=yourclient
}
case yourNumOfFollowers(x:Int,numOfUsers:Int)=>
{
this.x=x
while(i<=x)
{
y=util.Random.nextInt(numOfUsers)
if(!flist.contains(y) && y!=id)
{
flist+=y
}
i+=1
}
list=flist.toList
serverList(yourServer)!followers(id,list,yourclient)
}

case tweetsOfFollower(tlst:ListBuffer[String])=>{
      this.tlist=tlst	
      // println("the list from server:"+tlist)
    }

case startTweet()=>
{
y=util.Random.nextInt(100)+1
bombardTweets(y)
}
}
}