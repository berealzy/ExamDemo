package com.migu.schedule.info;

public class Task {

	private int taskId;
	private int consumption;
	private boolean inWork;
	
	public Task(int tId, int taskconsumption, boolean flag) {
		taskId = tId;
		consumption = taskconsumption;
		inWork = flag;
	}

	
	public int getTaskId() {
		return taskId;
	}
	public void setTaskId(int taskId) {
		this.taskId = taskId;
	}
	public int getConsumption() {
		return consumption;
	}
	public void setConsumption(int consumption) {
		this.consumption = consumption;
	}


	public boolean isInWork() {
		return inWork;
	}

	public void setInWork(boolean inWork) {
		this.inWork = inWork;
	}
	
    public String toString()
    {
        return "Task [taskId=" + taskId + ", consumption=" + consumption + "]";
    }
	
	
}
