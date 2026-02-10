RainStorm Stream Processing Framework

## Overview

RainStorm is a distributed stream processing framework designed to process large datasets in real-time across a cluster of machines. It employs a centralized Leader-Worker architecture to manage parallel data transformation stages.

## File Structure
stormruler.go: The Leader process (Source, Scheduler, Resource Manager).

rainstorm.go: The Worker process (Task Executor).

RainStormStructs/: Shared data structures for RPC communication.

failure_detection/: MP2 Gossip failure detector implementation.

hydfs_system/: MP3 Distributed File System implementation.

## Build Instructions

cd g51mp4

To build the leader and the workers:

go build Leader/StormRuler.go

go build RainStorm.go

To build the operators:

cd opexes

go build operators/operator_name.go

## How to Run

1. Start the Workers
On every worker machine (VMs 2-10), start the worker process. The worker will initialize the failure detector, start the HyDFS node, and listen for RPC instructions from the Leader.

./RainStorm

2. Start the Leader
On the Leader machine (VM 1), start the leader process.

./StormRuler

3. Once the Leader is running, you must manually trigger the discovery of available worker nodes before submitting jobs by typing "discover".

4. To trigger a new RainStorm application type:

RainStorm < Stages > < TasksPerStage > < Op1 > < Arg1 > ... < HydfsSrc > < HydfsDest > < ExactlyOnce > < Autoscale > < InputRate > < LW > < HW >

For example, to trigger 2 stage application with identity and transform type:

RainStorm 2 4 ../opexes/simpleiden “” ../opexes/transform “” ../datasets/dataset1.csv test.txt 0 0 0 0 0

# Available Commands

list_tasks: queries the leader process for task details. For each task process, outputs its VM, PID, op_exe, and local log file.

kill_task < TaskID >: given a task’s TaskID, abruptly kills the task process.
