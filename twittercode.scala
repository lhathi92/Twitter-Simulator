import akka.actor._
import scala.util.Random
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Queue
import com.typesafe.config.ConfigFactory
import java.net.InetAddress

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

object twittercode extends App
{
  override def main(args: Array[String]) :Unit= {
      
    //val noOfUsers:Int = args(0).toInt
    var numOfServers:Int = args(0).toInt
    var numOfClients:Int = args(1).toInt
    var numOfUsers:Int = args(2).toInt

    
    val hostIP = InetAddress.getLocalHost.getHostAddress()
	
	val config = ConfigFactory.parseString("""
	    akka {
			actor {
				provider = "akka.remote.RemoteActorRefProvider"
			}
			
			remote {
				enabled-transports = ["akka.remote.netty.tcp"]
				
				netty.tcp {
					hostname = """ + hostIP + """
					port = 2252
				}
			}
		}
	""")
	var numOfUsersPerServer:Int = numOfUsers/numOfServers

//val system = ActorSystem("actorSystem", ConfigFactory.load(config))
	var serverActorList:List[ActorRef] = Nil
	//var clientActorList:List[ActorRef] = Nil
	//var userActorList:List[ActorRef] = Nil
 
	val serversystem = ActorSystem("Server", ConfigFactory.load(config))
	//val clientsystem = ActorSystem("Client")
	//val usersystem = ActorSystem("User")
	var boss:ActorRef = serversystem.actorOf(Props(new Boss(serversystem)), "boss")
	var i:Int=0
	var timer: Long = System.currentTimeMillis
	var startTimer: Long = System.currentTimeMillis

	while(i<numOfServers){
		serverActorList ::= serversystem.actorOf(Props(new Servers(numOfServers, numOfUsersPerServer)))
				i += 1
	}

    i=0

    while(i<numOfServers){
    	serverActorList(i)!serverInit(i, numOfServers, numOfUsersPerServer,boss,serverActorList)
    	serverActorList(i)!getTime(timer)
    	i += 1
    }

    i=0
//while(i<numOfClients){
//clientActorList ::= clientsystem.actorOf(Props(new Clients))
//i += 1
//}
    
i=0

	var uids:Int=(numOfUsers/numOfClients)
/*while(i<numOfClients)
{
var rangeStart=((uids)*(i))
var rangeEnd=((uids)*(i+1))-1
clientActorList(i) ! sendRange(rangeStart,rangeEnd,numOfServers,numOfUsers,serverActorList)
i+=1
}*/

 var startTimer2: Long = System.currentTimeMillis
 //println("time ")
 
 boss!nums(numOfServers,serverActorList,numOfUsers)

 // boss ! trackTime(startTimer)
 
 	
  }

}

case class Boss(Server: ActorSystem) extends Actor{
 
  var b:Long=0
  var c:Int=0
  var start:Long=System.currentTimeMillis()
  var total:Int=0
  var serverc:Int=0
  var userc:Int=0
  var totaluc:Int=0
  var totalsc:Int=0
  var numOfServers:Int=0
  var serverActorList:List[ActorRef]=Nil
  var numOfUsers:Int=0
  
  def receive={
    
  case nums(numOfServers:Int,serverActorList:List[ActorRef],numOfUsers:Int)=>{
      this.numOfServers =numOfServers
      this.serverActorList=serverActorList
      this.numOfUsers=numOfUsers
      println("i got the info to send")
    }
  
  case "Connection Complete"=>{
    println("Connection done with:"+sender)
    sender!sendInfo(numOfServers:Int,serverActorList:List[ActorRef],numOfUsers:Int)
  }
  
  
  case trackTime(b:Long,userc:Int,serverc:Int) => {
      c+=1
      this.b = b
      this.serverc=serverc
      this.userc=userc
      totaluc+=userc
      totalsc+=serverc
     // total+=b
    }
  
  total=totaluc+totalsc
    
  /*  if((System.currentTimeMillis()-start)==10000)
    {
     println("time:"+(System.currentTimeMillis()-start)+" miliseconds and tweets:"+total) 
    }
    */
     if(c==numOfServers/100){
      println("half Done")
       println("time:"+(System.currentTimeMillis()-start)+" miliseconds and tweets:"+total)
    }
   
    if(c==numOfServers/2){
      println("half Done")
       println("time:"+(System.currentTimeMillis()-start)+" miliseconds and tweets:"+total)
    }
    
    if(c==numOfServers){
      println("Final total time:"+(System.currentTimeMillis()-start)+" miliseconds and total tweets:"+total)
      sys.exit()
    }
  }
  
  	if(c==numOfServers/2){
     println("half Done")
    //sys.exit()
  }
}

class userObject()
{
var uid:Int=0
var flist:List[Int] = List()
var queue = Queue.empty[String]
}

case class Servers(numOfServers:Int, numOfUsersPerServer:Int) extends Actor
{
  
	var id:Int=0
	var numOfServer:Int=1
	var userId:Int=0
	var i:Int=0
	var userActor:ActorRef=null
	var usersToServer:Map[Int,Int] = Map()
	var users=new Array[Int](numOfUsersPerServer)
	var tweet:String=null
	var userlist=new Array[userObject](numOfUsersPerServer)
	var count:Int=0
	var countTweetsPerServer:Int=0
	var countlists:Int=0
	var yourclient:ActorRef=null
	var flist:List[Int] = Nil
	var tl = new ListBuffer[String]()
	var s:String=null
	var numOfTweets:Int=1 
	var userref:ActorRef=null
	var serverActorList:List[ActorRef]=Nil
	var countend:Int=0
	var boss:ActorRef=null
	var startt:Long=0
	var tweetcount:Int=0
	var finalcount:Int=0
	var a = 0
	
for(a <-0 to numOfUsersPerServer-1)
{
users(a)=0
}

	def instantiateUsers(users:Array[Int], userActor:ActorRef)
	{
		var j:Int=0
		this.userActor=userActor
			for(j <-0 to users.length-1)
				{
				if(userlist(j)==null)
				{	
					var usr = new userObject()
					usr.uid=users(j)
					userlist(j)=usr
				}
		   }
	}

	def printUserList(userlist:Array[userObject])
	{
		var j:Int=0
				for(j <-0 to userlist.length-1)
					{
//println(userlist(j).uid +" "+ userlist(j).flist +" "+ userlist(j).queue)
					}
	}


	def receive ={

	case serverInit(id:Int, numOfServers:Int, numOfUsersPerServer:Int,boss:ActorRef,serverActorList:List[ActorRef])=>
	{
		this.id=id
		this.numOfServer=numOfServers
		this.serverActorList=serverActorList
		this.boss=boss
	}

	case getTime(b:Long)=>{
	this.startt=b
	}

	case userTweet(userId:Int,tweet:String)=>
	{
		tweetcount+=1
		var index:Int = userId/numOfServers
		
		if(userlist(index)==null)
		{
			var usr = new userObject()
			usr.uid=userId
			userlist(index)=usr
		}
		
		userlist(index).queue+=(tweet)
		countTweetsPerServer+=1

		if(countTweetsPerServer==50)
		{
			printUserList(userlist)
		}
	}

	case userList(usersToServer:Map[Int,Int])=>
	{
		this.usersToServer = usersToServer
		var j:Int=0
		this.userActor = sender
		usersToServer.keys.foreach{
		  i=>
		users(i/numOfServers)=i
		count+=1
		}

		if(count==numOfUsersPerServer)
		{
			instantiateUsers(users, sender)
		}
// sender! madeTheUsers()
	}

	case followers(userId:Int,list:List[Int],yourclient:ActorRef)=>
	{
		var ind:Int=userId/numOfServers
		this.yourclient=yourclient

		if(userlist(ind)==null)
		{
			var usr = new userObject()
			usr.uid=userId
			userlist(ind)=usr
		}

		userlist(ind).flist=list
		countlists+=1

		if(countlists==numOfUsersPerServer)
		{ 
			yourclient!madeTheUsers()
		}

    }

	case fetchTweet(userId:Int,list:List[Int])=>{
      var tlist:List[String] = Nil
      var tweetlist = new ListBuffer[String]()
      var temptweetlist = new ListBuffer[String]()
      var userref:ActorRef=sender
      this.userId=userId
      this.flist =list
      //println("entered fetch tweets")
      for(x<-list)
      {
         for(a<-0 to 100){
        if((x%numOfServers)==id){			//if follower data is in this server
         // println("entered internal if")
         
          if(!userlist(x/numOfServers).queue.isEmpty){
            tweetcount+=1
            tweetlist+=userlist(x/numOfServers).queue.dequeue
          }
       }
        else{
         // println("entered else")
       serverActorList(x%numOfServers)!sendMeTweets(x,numOfTweets,sender)		//contact server whoich has its data
        // temptweetlist= tweetlist++tl
        }
        sender!tweetsOfFollower(tweetlist)			//returns tweets of followers it owns
      }
         
      }  
 
    }
	
    case sendMeTweets(x:Int,numOfTweets:Int,userref:ActorRef)=>{
      val tweetlist = new ListBuffer[String]()
      
      this.userId=x
      this.numOfTweets=numOfTweets
      this.userref=userref
      if(userlist(x/numOfServers).queue.isEmpty){			//bad coding
      
      }
      else{													//keep on storing tweets of follower in the list buffer
        tweetcount+=1
        tweetlist+=userlist(x/numOfServers).queue.dequeue
      }
      userref!tweetsOfFollower(tweetlist)				    // send tweets to user who asked it (the user is not under this server)
    }
    
    case doneTweeting(c:Int)=>{
    
      countend+=1
      this.finalcount=c
      if(countend==numOfUsersPerServer%10){
      
        boss!trackTime(System.currentTimeMillis,finalcount,tweetcount)
      //  context.system.shutdown()
      }
    }    

}
}