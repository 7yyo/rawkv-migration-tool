package com.pingcap.controller;

import java.util.Timer;

import org.tikv.common.TiSession;

import com.pingcap.task.TaskInterface;

public interface ScannerInterface {
	public static Timer scannerTimer = new Timer();
	public void run(TiSession tiSession,TaskInterface cmdInterFace);
}
