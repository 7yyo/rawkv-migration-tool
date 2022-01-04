package com.pingcap.controller;

import org.tikv.common.TiSession;

import com.pingcap.task.TaskInterface;

public interface ScannerInterface {
	public void run(TiSession tiSession,TaskInterface cmdInterFace);
}
