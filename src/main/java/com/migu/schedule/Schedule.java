package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.Task;
import com.migu.schedule.info.TaskInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
*类名和方法不能修改
 */
public class Schedule {

private static Map<Integer, List<Task>> node_tasks = new HashMap<Integer, List<Task>>();
private static Map<Integer, List<Task>> node_tasks1 = new HashMap<Integer, List<Task>>();

private static List<Task> hangUpQue = new ArrayList<Task>();
private static List<Integer> serverNodes = new ArrayList<Integer>();

    public int init() {
    	node_tasks = new HashMap<Integer, List<Task>>();
    	hangUpQue = new ArrayList<Task>();
    	serverNodes = new ArrayList<Integer>();
        return ReturnCodeKeys.E001;
    }


    public int registerNode(int nodeId) {
    	if(nodeId <= 0)
    	{
    		return ReturnCodeKeys.E004;
    	}
    	if(serverNodes.contains(nodeId))
    	{
    		return ReturnCodeKeys.E005;
    	}
    	
    	synchronized (serverNodes)
        {
    	serverNodes.add(nodeId);
        }
    	
        return ReturnCodeKeys.E003;
    }

    public int unregisterNode(int nodeId) {
    	if(nodeId <= 0)
    	{
    		return ReturnCodeKeys.E004;
    	}
    	
        if(!serverNodes.contains(nodeId))
        {
        	return ReturnCodeKeys.E007;
        }
        
        // 如果无正在运行的任务则直接删除
        if(!node_tasks.containsKey(nodeId))
        {
        	synchronized (serverNodes)
            {
        	serverNodes.remove(Integer.valueOf(nodeId));
            }
        }
        // 有正在运行的任务 则将任务加入挂起队列再删除
        else
        {
        	List<Task> taskList = node_tasks.get(Integer.valueOf(nodeId));
        	if(taskList != null)
        	{
            	synchronized (hangUpQue) 
            	{
            		hangUpQue.addAll(taskList);
    			}
        	}
        	
        	synchronized (serverNodes) 
        	{
        		serverNodes.remove(Integer.valueOf(nodeId));
			}
        }
        
        return ReturnCodeKeys.E006;
    }


    public int addTask(int taskId, int consumption) {
        if(taskId <= 0)
        {
        	return ReturnCodeKeys.E009;
        }
        
        if(isInHangupQue(hangUpQue,taskId) || isInWorkMap(node_tasks,taskId))
        {
        	return ReturnCodeKeys.E010;
        }
        
        synchronized (serverNodes) 
    	{        	
        	hangUpQue.add(new Task(taskId,consumption,false));
		}
        
        return ReturnCodeKeys.E008;
    }


    public int deleteTask(int taskId) {
    	
        if(taskId <= 0)
        {
        	return ReturnCodeKeys.E009;
        }
        
        if(isInHangupQue(hangUpQue,taskId))
        {
        	synchronized (hangUpQue) 
        	{        	
        		for(Task task : hangUpQue)
            	{
            		if(task.getTaskId() == taskId)
        	    	  {
            			hangUpQue.remove(task);
            			break;
        	    	  }
            	}
    		}
        }
        else if(isInWorkMap(node_tasks,taskId))
        {
        	synchronized (node_tasks) 
        	{   
        		Set<Integer> keys =node_tasks.keySet();
            	for(Integer nodeId : keys)
            	{
            	     List<Task> tasks = node_tasks.get(nodeId);
            	     for(Task task : tasks)
            	     {
            	    	  if(task.getTaskId() == taskId)
            	    	  {
            	    		   tasks.remove(task);
            	    		   node_tasks.put(nodeId, tasks);
            	    		   break;
            	    	  }
            	     }
            	}        		        		
    		}
        }
        else
        {
        	return ReturnCodeKeys.E012;
        }
        
        return ReturnCodeKeys.E011;
    }


    public int scheduleTask(int threshold) {
    	if(threshold <= 0)
    		return ReturnCodeKeys.E002;
    	
    	List<Task> allTasks = new ArrayList<Task>();
    	if(!node_tasks.isEmpty()){
    		Set<Integer> keys =node_tasks.keySet();
        	for(Integer nodeId : keys)
        	{
        		allTasks.addAll(node_tasks.get(nodeId));
        	}
    	}
    	if(!hangUpQue.isEmpty()){
    		allTasks.addAll(hangUpQue);
    	}
    	
    	//将其按资源消耗率从大到小进行排列
		Collections.sort(allTasks, new Comparator<Task>() {
			public int compare(Task t1, Task t2) {
				if (t1.getConsumption() > t2.getConsumption()) {
					return -1;
				} else if (t1.getConsumption() < t2.getConsumption()) {
					return 1;
				}
				return 0;
			}
		});
		
		int serverNodeNum = serverNodes.size();
		int avg = calSumConsumption(allTasks)/serverNodeNum;
		 divideTask(allTasks,serverNodeNum,true,avg);
		Set<Integer>  nodes = node_tasks1.keySet();
    	
    	for(Integer index : nodes){
    		System.out.println("index:" + index);
    		int i = index-1;
    		System.out.println("i:" + i);
    		System.out.println("serverNodes.get(i):" + serverNodes.get(i));
    		System.out.println("tasks.get(index)" + node_tasks1.get(index));
    	    node_tasks.put(serverNodes.get(i), node_tasks1.get(index));    	
    	}
        return ReturnCodeKeys.E013;
    }


    public int queryTaskStatus(List<TaskInfo> tasks) {
    	if(tasks == null)
    		return ReturnCodeKeys.E016;
    	tasks = new ArrayList<TaskInfo>();
    	Set<Integer>  nodes = node_tasks.keySet();
    	for(Integer index : nodes){   		
    		List<Task> list = node_tasks.get(index);
    		for(Task task :list){
    			TaskInfo taskinfo= new TaskInfo();
    			taskinfo.setNodeId(index);
    			taskinfo.setTaskId(task.getTaskId());
    			tasks.add(taskinfo);
    		}
    	}
    	
        return ReturnCodeKeys.E015;
    }
    
    
    private boolean isInWorkMap(Map<Integer, List<Task>> list, int taskId)
    {
    	Set<Integer> keys =list.keySet();
    	for(Integer nodeId : keys)
    	{
    	     List<Task> tasks = list.get(nodeId);
    	     for(Task task : tasks)
    	     {
    	    	  if(task.getTaskId() == taskId)
    	    	  {
    	    		  return true;
    	    	  }
    	     }
    	}
    	return false;
    }
    
    private boolean isInHangupQue(List<Task> list, int taskId)
    {
    	for(Task task : list)
    	{
    		if(task.getTaskId() == taskId)
	    	  {
	    		  return true;
	    	  }
    	}
    	return false;
    }

    
	private static void divideTask(List<Task> dList, Integer num, boolean direction, int avg) {
		int[] nodeGetAvg = new int[num+1];
		if (dList.size() >= num) {
			for (int i = 0; i < num; i++) {
				Integer index;
				if (direction) {
					index = i + 1;
					do{
					List<Task> list = node_tasks1.get(index);
					if(nodeGetAvg[index]==1 || calSumConsumption(list) > avg)
					{
						nodeGetAvg[index] = 1;	
						index++;
					}
					else{
						if(list == null){
							list = new ArrayList<Task>();
						}
						list.add(dList.get(i));
						dList.get(i).setInWork(true);
						node_tasks1.put(index, list);
					}
					
					}while(dList.get(i).isInWork() && index <=num);	
					
				} else {
					index = num - i;
					do{
						List<Task> list = node_tasks1.get(index);
						if(nodeGetAvg[index]==1 || calSumConsumption(list) > avg)
						{
							nodeGetAvg[index] = 1;	
							index--;
						}
						else{	
							if(list == null){
								list = new ArrayList<Task>();
							}
							list.add(dList.get(i));
							dList.get(i).setInWork(true);
							node_tasks1.put(index, list);
						}
						
						}while(dList.get(i).isInWork()&& index >= 1);
				}									
			}
			
			// 去除已经分配的
			List<Task> newTaskList = new ArrayList<Task>();
			for (int i = 0; i < dList.size(); i++) {
				Task task = dList.get(i);
				if (!task.isInWork()) {
					newTaskList.add(dList.get(i));
				}
			}
			if (newTaskList.size() > 0) {
				// 下次分配，按反方向分配
				divideTask(newTaskList, num, !direction, avg);
			}
		
		} else if (dList.size() < num && direction) {
			
			for (int i = 0; i < dList.size(); i++) {
				int index = i;
				do{
				List<Task> list = node_tasks1.get(index+1);
				if(nodeGetAvg[index+1]==1 || calSumConsumption(list) > avg)
				{
					nodeGetAvg[index+1] = 1;	
					index++;
				}
				else{	
					if(list == null){
						list = new ArrayList<Task>();
					}
					list.add(dList.get(i));
					dList.get(i).setInWork(true);
					node_tasks1.put(index+1, list);
				}
				
				}while(dList.get(i).isInWork() && index < num);	
				
			}
		}
		else if (dList.size() < num && !direction){
			
			for (int i = 0; i < dList.size(); i++) {
				int index = num;
				do{
				List<Task> list = node_tasks1.get(index-i);
				if(nodeGetAvg[index-i]==1 || calSumConsumption(list) > avg)
				{
					nodeGetAvg[index-i] = 1;	
					index--;
				}
				else{	
					if(list == null){
						list = new ArrayList<Task>();
					}
					list.add(dList.get(i));
					dList.get(i).setInWork(true);
					node_tasks1.put(index-i, list);
				}
				
				}while(dList.get(i).isInWork() && index-i >0 );
			}
		}
	}


	private static int calSumConsumption(List<Task> list) {
		if(list == null)
			return 0;
		int sum = 0;
		for(int i = 0; i< list.size(); i++){
			sum +=list.get(i).getConsumption();
		}
		return sum;
	}
	
}
