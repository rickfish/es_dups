package com.bcbsfl.es;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;

public class Main {

	public static void main(String[] args) {
		Utils.init();
		String appToRun = Utils.getProperty("which.app");
		System.out.println("Running " + appToRun);
		try {
			switch(appToRun) {
			case "TaskReconciler":
				if(Utils.getBooleanProperty("run.continuously")) {
					runContinuously();
				} else {
					new TaskReconciler().reconcile();
				}
				break;
			case "WorkflowReconciler":
				if(Utils.getBooleanProperty("run.continuously")) {
					runContinuously();
				} else {
					new WorkflowReconciler().reconcile();
				}
				break;
			default:
				System.out.println("******* The which.app variable is set to '" + appToRun + "' which is not a valid option.");
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
		while(true) {
			System.out.println("Since it is possible that we are running in Openshift, we will keep sleeping for an hour to keep the pod running...");
			try {
				Thread.sleep(3600000);
			} catch(Exception e) {
			}
		}
	}
	static private void runContinuously() throws Exception {
		System.out.println("This will be run contiunously");
		String startDateString = Utils.getProperty("db.startTimeframe");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Calendar startDate = null;
		if(StringUtils.isBlank(startDateString)) {
			startDate = Calendar.getInstance();
			startDate.add(Calendar.DATE, -1);
			startDate.set(Calendar.HOUR_OF_DAY, 0);
			startDate.set(Calendar.MINUTE, 0);
			startDate.set(Calendar.SECOND, 0);
			startDate.set(Calendar.MILLISECOND, 0);
			startDateString = sdf.format(startDate.getTime());
			System.setProperty("db.startTimeframe", startDateString);
		}
		String dateComponents[] = startDateString.split("-");
		if(dateComponents.length != 3) {
			throw new Exception("db.startTimeframe needs to be specified in yyyy-mm-dd format to run continuously");
		}
		if(startDate == null) {
			startDate = Calendar.getInstance();
			startDate.setTime(sdf.parse(startDateString));
			startDate.set(Calendar.HOUR_OF_DAY, 0);
			startDate.set(Calendar.MINUTE, 0);
			startDate.set(Calendar.SECOND, 0);
			startDate.set(Calendar.MILLISECOND, 0);
		}
		while(true) {
			while(true) {
				Calendar today = Calendar.getInstance();
				today.set(Calendar.HOUR_OF_DAY, 0);
				today.set(Calendar.MINUTE, 0);
				today.set(Calendar.SECOND, 0);
				today.set(Calendar.MILLISECOND, 0);
				if(today.after(startDate)) {
					System.setProperty("db.startTimeframe", sdf.format(startDate.getTime()));
					break;
				}
				System.out.println("[" + new Date().toString() + "] Today is not greater than " + startDate.getTime().toString() + ", waiting for an hour, will check if it is time to run again.");
				try {
					Thread.sleep(3600000);
				} catch(Exception e) {
				}
			}
			try {
				ReconcileApp reconciler = null;
				reconciler = new TaskReconciler();
				reconciler.reconcile();
				reconciler = new WorkflowReconciler();
				reconciler.reconcile();
				startDate.add(Calendar.DATE, 1);
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
}
