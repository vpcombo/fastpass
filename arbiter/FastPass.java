
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

public class FastPass implements Runnable
{
	private static Queue<String> requests;
	private static HashMap<String,Long> last_assigned;
	private static PriorityQueue<Pair> wait_list_timeslot;
	private static Queue<Set<Pair>> wait_list_route;
	
	private static Set<Pair> schedule_later;
	
	public static final int INITIAL_CAPACITY = 10;
	public static final String DELIMITER = ":";
	public static final long MAX_TIME = 1000000000L;
	public static final long timeslot_cap = 1000;
	public static final long route_cap = 1000;
	public static final long request_cap = 1000;
	
	long last_checkpoint_time;
	long last_checkpoint_timeslot;
	
	public FastPass()
	{
		requests = new LinkedList<String>();
		last_assigned = new HashMap<String,Long>();
		wait_list_timeslot = new PriorityQueue<Pair>(INITIAL_CAPACITY, new PairComparator());
		wait_list_route = new LinkedList<Set<Pair>>();
		schedule_later = new HashSet<Pair>();
	}
	
	@Override
	public void run()
	{
		Thread s = new Thread(new Server());
		Thread tss = new Thread(new TimeSlotScheduler());
		Thread ae = new Thread(new AddressExtractor());
		Thread rs = new Thread(new RouteScheduler());
		System.out.println("starting server");
		s.start();
		System.out.println("starting addess extractor");
		ae.start();
		System.out.println("starting timeslot scheduler");
		tss.start();
		System.out.println("starting route scheduler");
		rs.start();
		/*
		StaticFlowPusher sfp = new StaticFlowPusher();
		sfp.run();
		*/
	}
	
	class Server implements Runnable
	{
		@Override
		public void run() 
		{
			int source;
			int dest;
			while (true)
			{
				source = -1;
				dest = -1;
				while (source == -1 || source == 17)
					source = (int) (Math.random() * 16 + 1);
				while (dest == -1 || dest == 17 || dest == source)
					dest = (int) (Math.random() * 16 + 1);
				while (FastPass.addRequest(source + DELIMITER + dest) == false);
				//System.out.println(source + " " + dest);
			}
		}
	}
	
	class AddressExtractor implements Runnable
	{
		String current_request;
		String [] addresses;
		Pair to_add;
		long to_add_last_assigned;
		
		@Override
		public void run() 
		{
			while (true)
			{
				to_add_last_assigned = 0L;
				current_request = null;
				addresses = null;
				to_add = null;
				while ((current_request = FastPass.getRequest()) == null);
				addresses = current_request.split(DELIMITER);
				if (addresses.length != 2)
					continue;
				to_add = new Pair(addresses[0], addresses[1]);
				if (last_assigned.get(current_request) == null)
				{
					to_add.last_assigned = last_checkpoint_timeslot;
					last_assigned.put(current_request, to_add.last_assigned);
				}
				else
					to_add.last_assigned = last_assigned.get(current_request);
				while (FastPass.addToWaitListTimeslot(to_add) == false);
			}
		}
	}
	
	class TimeSlotScheduler implements Runnable
	{
		int base_timeslot;
		HashMap<String,Integer> pair_timeslot_bitstrings;
		HashMap<Long,Set<Pair>> send_to_route_scheduler;
		
		public TimeSlotScheduler()
		{
			pair_timeslot_bitstrings = new HashMap<String,Integer>();
			send_to_route_scheduler = new HashMap<Long,Set<Pair>>();
			last_checkpoint_time = System.nanoTime();
			last_checkpoint_timeslot = 0;
		}
		@Override
		public void run()
		{
			Pair curr;
			String curr_string;
			Iterator<String> keys;
			int runningValue;
			int test_zero;
			int offset;
			boolean start;
			int timeslot_offset;
			while (true)
			{
				curr = null;
				curr_string = null;
				runningValue = 0;
				test_zero = 1;
				offset = 1;
				start = false;
				timeslot_offset = 0;
				while ((curr = FastPass.removeFromWaitListTimeslot()) == null)
				{
					//System.out.println(curr);
					updateCheckpoint();
				}
				//System.out.println(curr);
				curr_string = curr.src + DELIMITER + curr.dest;
				if (!(pair_timeslot_bitstrings.containsKey(curr_string)))
				{
					pair_timeslot_bitstrings.put(curr_string, 0);
				}
				//System.out.println("here?");
				keys = pair_timeslot_bitstrings.keySet().iterator();
				String nextkey;
				while (keys.hasNext())
				{
					nextkey = keys.next();
					if (nextkey.equals(curr_string))
						continue;
					runningValue = runningValue & pair_timeslot_bitstrings.get(nextkey);
					//System.out.println(runningValue);
				}
				int index = 0;
				while (test_zero == 1 && index < 32)
				{
					test_zero = runningValue & 1;
					runningValue = runningValue >> 1;
					if (start == false)
					{
						offset = 1;
					}
					else
					{
						offset = offset << 1;
						//System.out.println("Offset:" + offset);
						timeslot_offset++;
					}
					start = true;
					index++;
				}
				//System.out.println(test_zero);
				if (test_zero == 1 && index >= 32)
				{
					//System.out.println("entered 1");
					schedule_later.add(curr);
				}
				else
				{
					//System.out.println("entered 2");
					
					if (test_zero == 1 && start == false)
					{
						//System.out.println("entered 3");
						offset = 1;
						timeslot_offset = 0;
					}
					//System.out.println("last checkpoint" + last_checkpoint_timeslot);
					//System.out.println("timeslot_offset" + timeslot_offset);
					curr.last_assigned = last_checkpoint_timeslot + (long) timeslot_offset;
					pair_timeslot_bitstrings.put(curr_string, pair_timeslot_bitstrings.get(curr_string) | offset);
					if (send_to_route_scheduler.containsKey(curr.last_assigned) == false)
					{
						send_to_route_scheduler.put(curr.last_assigned, new HashSet<Pair>());
					}
					//System.out.println("Last assigned: " + curr.last_assigned);
					send_to_route_scheduler.get(curr.last_assigned).add(curr);
					updateCheckpoint();
				}
			}
		}
		public boolean updateCheckpoint()
		{
			//System.out.println("no bottleneck");
			long curr_time = System.nanoTime();
			long diff;
			long schedule_route;
			Set<Pair> curr_timeslot;
			Iterator<String> keys;
			Iterator<Pair> put_backs;
			if ((diff = curr_time - last_checkpoint_time) >= MAX_TIME)
			{
				//System.out.println("changing of the guard");
				schedule_route = diff / MAX_TIME;
				for (int i = 0; i < schedule_route; i++)
				{
					if ((curr_timeslot = send_to_route_scheduler.get(last_checkpoint_timeslot + i)) != null)
					{
						while (FastPass.addToWaitListRoute(curr_timeslot) == false);
						send_to_route_scheduler.remove(last_checkpoint_timeslot + i);
					}
					
				}
				
				put_backs = schedule_later.iterator();
				Pair curr_pair;
				while (put_backs.hasNext())
				{
					curr_pair = put_backs.next();
					wait_list_timeslot.add(curr_pair);
				}
				schedule_later.clear();
				
				keys = pair_timeslot_bitstrings.keySet().iterator();
				int curr;
				String currKey;
				while (keys.hasNext())
				{
					currKey = keys.next();
					curr = pair_timeslot_bitstrings.get(currKey);
					curr = curr >> schedule_route;
					pair_timeslot_bitstrings.put(currKey, curr);
				}
				last_checkpoint_time = curr_time;
				last_checkpoint_timeslot = last_checkpoint_timeslot + schedule_route; 
				//System.out.println("done");
				return true;
			}
			else
			{
				//System.out.println("done");
				return false;
			}
		}
	}
	
	class RouteScheduler implements Runnable
	{
		@Override
		public void run() 
		{
			Set<Pair> curr = null;
			Iterator<Pair> it = null;
			Pair next;
			while (true)
			{
				curr = null;
				it = null;
				while ((curr = FastPass.removeFromWaitListRoute()) == null);
				it = curr.iterator();
				while (it.hasNext())
				{
					next = it.next();
					System.out.println("Source: " + next.src + " Destination: " + next.dest + " Timeslot: " + next.last_assigned);
				}
			}
		}
	}
	/*
	class StaticFlowPusher implements Runnable
	{
		@Override
		public void run() 
		{
			// TODO Auto-generated method stub
			
		}
	}*/
	
	public static synchronized boolean addRequest(String pair)
	{
		if (requests.size() < request_cap)
			return requests.add(pair);
		else return false;
	}
	
	public static synchronized String getRequest()
	{
		return requests.poll();
	}
	
	public static synchronized boolean addToWaitListTimeslot(Pair p)
	{
		if (wait_list_timeslot.size() < timeslot_cap)
			return wait_list_timeslot.add(p);
		else return false;
	}
	
	public static synchronized Pair removeFromWaitListTimeslot()
	{
		return wait_list_timeslot.poll();
	}
	
	public static synchronized boolean addToWaitListRoute(Set<Pair> sp)
	{
		if (wait_list_route.size() < route_cap)
			return wait_list_route.add(sp);
		else return false;
	}
	
	public static synchronized Set<Pair> removeFromWaitListRoute()
	{
		return wait_list_route.poll();
	}
}
