package edu.sjsu.cs185c.here;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;


import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.StatusRuntimeException;

import edu.sjsu.cs249.chain.ChainDebugGrpc.ChainDebugImplBase;
import edu.sjsu.cs249.chain.ChainDebugRequest;
import edu.sjsu.cs249.chain.ChainDebugResponse;
import edu.sjsu.cs249.chain.ExitRequest;
import edu.sjsu.cs249.chain.ExitResponse;

import edu.sjsu.cs249.chain.HeadChainReplicaGrpc.HeadChainReplicaImplBase;
import edu.sjsu.cs249.chain.IncRequest;
import edu.sjsu.cs249.chain.HeadResponse;

import edu.sjsu.cs249.chain.ReplicaGrpc.ReplicaImplBase;
import edu.sjsu.cs249.chain.ReplicaGrpc;
import edu.sjsu.cs249.chain.UpdateRequest;
import edu.sjsu.cs249.chain.UpdateResponse;
import edu.sjsu.cs249.chain.AckRequest;
import edu.sjsu.cs249.chain.AckResponse;
import edu.sjsu.cs249.chain.StateTransferRequest;
import edu.sjsu.cs249.chain.StateTransferResponse;
import edu.sjsu.cs249.chain.ReplicaGrpc.ReplicaBlockingStub;

import edu.sjsu.cs249.chain.TailChainReplicaGrpc.TailChainReplicaImplBase;
import edu.sjsu.cs249.chain.GetRequest;
import edu.sjsu.cs249.chain.GetResponse;

import java.util.*;
public class Main {
    static ZooKeeper zk;
    static String path;
    static String ip;
    static ReplicaBlockingStub predecessorStub;
    static ReplicaBlockingStub successorStub;
    static ManagedChannel predecessorChannel;
    static ManagedChannel successorChannel;
    static String prevSucc = "";
    static String prevPred = "";
    static boolean init = false;
    static boolean head;
    static boolean tail;
    static int xid = 0;
    static int lxid;
    static ConcurrentHashMap<String, Integer> data = new ConcurrentHashMap<>();
    static ConcurrentHashMap<Integer, UpdateRequest> ackList = new ConcurrentHashMap<>();
    static ConcurrentHashMap<Integer, StreamObserver<HeadResponse>> headResponser = new ConcurrentHashMap<>();
    static ReentrantLock statelock = new ReentrantLock();
    static ReentrantLock queuelock = new ReentrantLock();
    static PriorityQueue<Integer> ackQueue = new PriorityQueue<>();
    static PriorityBlockingQueue<UpdateRequest> updateQueue = new PriorityBlockingQueue<UpdateRequest>(20, new Comparator<UpdateRequest>() {
        public int compare(UpdateRequest r1, UpdateRequest r2) {
            return Integer.compare(r1.getXid(), r2.getXid());
        }
    });

    static class stateChange implements Watcher {
        @Override
        public void process(WatchedEvent e) {
            if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                boolean transfer = false;
                ArrayList<String> replicas = null;
                while (replicas == null) {
                    try {
                        replicas = new ArrayList<>(zk.getChildren(path, new stateChange()));
                    } catch (Exception e1) {replicas = null;}
                }
                Collections.sort(replicas);
                int myIndex = -1;
                String childPath;
                String childData = null;

                for (int i = 0; i < replicas.size(); i++ ) {
                    childPath = path + "/" + replicas.get(i);
                    while (childData == null) {
                        try {
                            childData = new String(zk.getData(childPath, false, null));
                        } catch (Exception e1) {childData = null;}
                    }
                    if (childData.split("\n")[0].equals(ip)) {
                        myIndex = i;
                        break;
                    } 
                    childData = null;
                }
                System.out.println(myIndex);
                if (myIndex < replicas.size() - 1 && myIndex > -1) {
                    String successorPath = path + "/" + replicas.get(myIndex + 1);
                    String successorIP = null;
                    while (successorIP == null) {
                        try {
                            successorIP = new String (zk.getData(successorPath,false,null));
                        } catch (Exception e1) {successorIP = null;}
                    }
                    if (!prevSucc.equals(replicas.get(myIndex + 1))) {
                        successorChannel = ManagedChannelBuilder.forTarget(successorIP.split("\n")[0]).usePlaintext().build();
                        prevSucc = replicas.get(myIndex + 1);
                        successorStub = ReplicaGrpc.newBlockingStub(successorChannel);
                        tail = false;
                        transfer = true;
                        System.out.println(replicas.get(myIndex + 1) + " is successor");
                    }
                } else {
                    tail = true;
                    System.out.println("I am the tail");
                    for (int i: ackList.keySet()) {
                        ackQueue.add(i);
                        ackList.remove(i);
                    }
                }

                if (myIndex > 0) {
                    String predecessorPath = path + "/" + replicas.get(myIndex - 1);
                    String predecessorIP = null;
                    while (predecessorIP == null) {
                        try {
                            predecessorIP = new String (zk.getData(predecessorPath,false,null));
                        } catch (Exception e1) {predecessorIP = null;}
                    }
                    if (!prevPred.equals(replicas.get(myIndex - 1))) {
                        predecessorChannel = ManagedChannelBuilder.forTarget(predecessorIP.split("\n")[0]).usePlaintext().build();
                        prevPred = replicas.get(myIndex - 1);
                        predecessorStub = ReplicaGrpc.newBlockingStub(predecessorChannel);
                        head = false;
                        System.out.println(replicas.get(myIndex - 1) + " is predecessor");
                    }
                } else {
                    System.out.println("I'm the head");
                    head = true;
                }
                if (transfer) {
                    transferState();
                }
            }
        }
    }

    static class ReplicaImpl extends ReplicaImplBase {

        @Override
        public void update(UpdateRequest request, StreamObserver<UpdateResponse> responseObserver) {
            System.out.printf("Received update with xid %d\n", request.getXid());
            UpdateResponse response;

            if (!init) {
                response = UpdateResponse.newBuilder().setRc(1).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }

            String key = request.getKey();
            int value = request.getNewValue();

            statelock.lock();
            System.out.println("State lock obtained in update request");
            try {
                data.put(key, value);
                xid = request.getXid();
                if (tail) {
                    System.out.println("Updated lxid because I'm tail");
                    lxid = request.getXid();
                    System.out.println("In update request sending to ack queue");
                    ackQueue.add(request.getXid());
                } else {
                    System.out.println("In update request sending to update queue");
                    updateQueue.add(request);
                }
            } finally {statelock.unlock();}
            System.out.println("State lock released in update request");

           
           
           
            response = UpdateResponse.newBuilder().setRc(0).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void stateTransfer(StateTransferRequest request, StreamObserver<StateTransferResponse> responseObserver) {
            StateTransferResponse response;
            System.out.println("Received a state transfer");

            if (init) {
                response = StateTransferResponse.newBuilder().setRc(1).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }

            for (UpdateRequest update: request.getSentList()) {
                ackQueue.add(update.getXid());
            }
            
            data = new ConcurrentHashMap<>(request.getStateMap());
            statelock.lock();
            try {
                System.out.println("updated lxid in state transfer");
                lxid = request.getXid();
            } finally {statelock.unlock();}
            init = true;
            response = StateTransferResponse.newBuilder().setRc(0).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void ack(AckRequest request, StreamObserver<AckResponse> responseObserver) {
            System.out.printf("Received an ack with xid %d", request.getXid());
            statelock.lock();
            try {
                ackList.remove(request.getXid());
            } finally {statelock.unlock();}

            if (head) {
                var headObserver = headResponser.get(request.getXid());
                var headResponse = HeadResponse.newBuilder().setRc(0).build();
                headObserver.onNext(headResponse);
                headObserver.onCompleted();
                headResponser.remove(request.getXid());
                System.out.println("Head should have responded");
                responseObserver.onNext(AckResponse.newBuilder().build());
                responseObserver.onCompleted();
                return; 
            }
            
            ackQueue.add(request.getXid());
            responseObserver.onNext(AckResponse.newBuilder().build());
            responseObserver.onCompleted();
        }
    }

    static class DebugImpl extends ChainDebugImplBase {

        @Override
        public void debug(ChainDebugRequest request, StreamObserver<ChainDebugResponse> responseObserver) {
            ChainDebugResponse response;
            statelock.lock();
            System.out.println("Debug obtained lock");
            try {
                response = ChainDebugResponse.newBuilder().putAllState(data).setXid(lxid).addAllSent(ackList.values()).build();
            } finally {statelock.unlock();}
            System.out.println("Debug released lock");
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }   

        @Override
        public void exit(ExitRequest request, StreamObserver<ExitResponse> responseObserver) {
            responseObserver.onNext(ExitResponse.newBuilder().build());
            responseObserver.onCompleted();
            System.exit(0);
        }
    }

    static class HeadImpl extends HeadChainReplicaImplBase {
        @Override
        public void increment(IncRequest request, StreamObserver<HeadResponse> responseObserver) {
            System.out.println("Received a head increment request");
            HeadResponse response; 
            if (!head) {
                response = HeadResponse.newBuilder().setRc(1).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }

            String key = request.getKey();
            int incValue = request.getIncValue();
            UpdateRequest updateRequest;
            statelock.lock();
            System.out.println("Increment request obtained state lock");
            try {
                xid += 1;
                if (data.containsKey(key)) 
                    data.put(key,data.get(key) + incValue);
                else
                    data.put(key,incValue);
                updateRequest = UpdateRequest.newBuilder().setKey(key).setNewValue(data.get(key)).setXid(xid).build();
                if (!tail)
                    updateQueue.add(updateRequest);
            } finally {statelock.unlock();}
            System.out.println("Increment request released state lock");

          
            if (tail) {
                System.out.println("updated lxid because I am the only node");
                lxid = updateRequest.getXid();
                response = HeadResponse.newBuilder().setRc(0).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }
            
            System.out.println("Head putting into update queue");
            headResponser.put(updateRequest.getXid(),responseObserver);
                   

        }
    }

    static class TailImpl extends TailChainReplicaImplBase {
        @Override
        public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
            System.out.println("Got a tail read request");
            GetResponse response;
            
            if (!tail) {
                response = GetResponse.newBuilder().setRc(1).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }
            
            statelock.lock();
            System.out.println("Tail request obtained state lock");
            try {
                if (data.containsKey(request.getKey())) 
                    response = GetResponse.newBuilder().setRc(0).setValue(data.get(request.getKey())).build();
                else 
                    response = GetResponse.newBuilder().setRc(0).setValue(0).build();
            } finally {statelock.unlock();}

            System.out.println("Tail request released state lock");
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    static void sendUpdate(UpdateRequest request) {
        
        UpdateResponse response = null;
        statelock.lock();
        try {
            ackList.put(request.getXid(),request);
            lxid = request.getXid();
            System.out.println("updated lxid because I sending an update");
        } finally {statelock.unlock();}

        while (response == null) {
            System.out.println("Attempting to send update");
            try {
                if (tail) {
                    return;
                }
                successorStub.withDeadlineAfter(10, TimeUnit.SECONDS);
                response = successorStub.update(request);
            } catch (StatusRuntimeException e) {
                response = null;
                System.out.println("sending update failed");
                System.out.println("going to sleep");
                try {
                    Thread.sleep(300);
                } catch (Exception e1) {}
            }
        }
        
        if (response.getRc() == 1) 
            transferState();
    }


    static void transferState() {
        StateTransferRequest request;
        StateTransferResponse response = null;
        statelock.lock();
        System.out.println("state transfer obtained state lock");
        try {
            request = StateTransferRequest.newBuilder().putAllState(data).setXid(lxid).addAllSent(ackList.values()).build();
        } finally {statelock.unlock();}
        System.out.println("state transfer released state lock");
        while (response == null) {
            System.out.println("Attempting to transfer state");
            try {
                if (tail) {
                    return;
                }
                successorStub.withDeadlineAfter(5, TimeUnit.SECONDS);
                System.out.printf("Sending update xid %d", request.getXid());
                response = successorStub.stateTransfer(request);
            } catch (StatusRuntimeException e) {
                response = null;
                System.out.println("Something went wrong");
                System.out.println("going to sleep");
                try {
                    Thread.sleep(300);
                } catch (Exception e1) {}
            }
        }
       
    }
    
    static void sendAck(int id) {
        AckResponse response = null;
        AckRequest request = AckRequest.newBuilder().setXid(id).build();
        while (response == null) {
            System.out.println("Attempting to send ack request");
            try {
                if (head) {
                    System.out.println("Im the head in ack");
                    if (headResponser.containsKey(id)) {
                        var headObserver = headResponser.get(id);
                        var headResponse = HeadResponse.newBuilder().setRc(0).build();
                        headResponser.remove(id);
                        headObserver.onNext(headResponse);
                        headObserver.onCompleted();
                    }
                    return;
                }
                predecessorStub.withDeadlineAfter(5, TimeUnit.SECONDS);
                System.out.printf("Sending ack xid %d", request.getXid());
                response = predecessorStub.ack(request);
            } catch (StatusRuntimeException e) {
                response = null;
                System.out.println("Something went wrong");
                System.out.println("going to sleep");
                try {
                    Thread.sleep(300);
                } catch (Exception e1) {}
            }
        }
    }
    
    @Command(name = "command parse")
    static class CommandLineInput implements Callable<Integer> {
        @Parameters(index = "0") String serverList;
        @Parameters(index = "1") String zpath;
        @Parameters(index = "2") String hostport;

        @Override
        public Integer call() throws Exception {
            path = zpath;
            ip = hostport;
            startUp(serverList, zpath, hostport);
            return 0;
        }
    }

    static void startUp(String serverList, String zpath, String hostport) throws Exception{
        zk = new ZooKeeper(serverList, 10000, (e) -> {System.out.println(e);});
        String nodeInfo = hostport + "\nHanyu";
        boolean success = false;
        while (!success) {
            try {
                zk.create(zpath + "/replica-", nodeInfo.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                success = true;
            } catch (Exception e) {
                ArrayList<String> children = null;
                try {
                    while (children == null) {
                        children = new ArrayList<>(zk.getChildren(zpath,new stateChange()));
                        for (String s: children) {
                            String childData = null;
                            String childPath = zpath + "/" + s;
                            while (childData == null) {
                                try {
                                    childData = new String(zk.getData(childPath, false, null));
                                    if (hostport.equals(childData.split("\n")[0])) {
                                        success = true;
                                    }
                                } catch (Exception e2) {childData = null;}
                            }
                        }
                    }
                } catch (Exception e1) {children = null;}
            }
        }

       
        System.out.println("My replica created");
        checkReplicaState();
        server();
    }

    static void server() {
        int port = Integer.parseInt(ip.substring(ip.indexOf(":") + 1, ip.length()));
        var server = ServerBuilder.forPort(port).addService(new ReplicaImpl()).
        addService(new DebugImpl()).addService(new TailImpl()).addService(new HeadImpl()).build();
        try {
            server.start();
            System.out.println("Server started");
            while (!server.isShutdown()) {
                if (!updateQueue.isEmpty()) {
                    var r = updateQueue.poll();
                    System.out.println("Sending update xid " + r.getXid());
                    sendUpdate(r);
                }
                if (!ackQueue.isEmpty())
                    sendAck(ackQueue.poll());
            }
        } catch (Exception e) {}
    }
    
    static void checkReplicaState() {
        ArrayList<String> replicas = null;
        while (replicas == null) {
            try {
                replicas = new ArrayList<>(zk.getChildren(path, new stateChange()));
            } catch (Exception e) {replicas = null;}
        }
        Collections.sort(replicas);
        int myIndex = -1;
        String childPath;
        String childData = null;

        System.out.println(replicas);
        for (int i = 0; i < replicas.size(); i++ ) {
            childPath = path + "/" + replicas.get(i);
            while (childData == null) {
                try {
                    childData = new String(zk.getData(childPath, false, null));
                } catch (Exception e) {childData = null;}
            }
            
            if (childData.split("\n")[0].equals(ip)) {
                myIndex = i;
                break;
            } 
            childData = null;
        }
        System.out.println(myIndex);

        if (myIndex < replicas.size() - 1 && myIndex > -1) {
            String successorPath = path + "/" + replicas.get(myIndex + 1);
            String successorIP = null;
            while (successorIP == null) {
                try {
                    successorIP = new String (zk.getData(successorPath,false,null));
                } catch (Exception e) {successorIP = null;}
            }

            successorChannel = ManagedChannelBuilder.forTarget(successorIP.split("\n")[0]).usePlaintext().build();
            prevSucc = replicas.get(myIndex + 1);
            successorStub = ReplicaGrpc.newBlockingStub(successorChannel);
            tail = false;
            System.out.println(replicas.get(myIndex + 1) + " is successor");
        } else {
            tail = true;
            System.out.println("I am the tail");
            for (int i: ackList.keySet()) {
                ackQueue.add(i);
                ackList.remove(i);
            }
        }

        if (myIndex > 0) {
            String predecessorPath = path + "/" + replicas.get(myIndex - 1);
            String predecessorIP = null;
            while (predecessorIP == null) {
                try {
                    predecessorIP = new String (zk.getData(predecessorPath,false,null));
                } catch (Exception e) {predecessorIP = null;}
            }
            predecessorChannel = ManagedChannelBuilder.forTarget(predecessorIP.split("\n")[0]).usePlaintext().build();
            prevPred = replicas.get(myIndex - 1);
            predecessorStub = ReplicaGrpc.newBlockingStub(predecessorChannel);
            head = false;
            System.out.println(replicas.get(myIndex - 1) + " is predecessor");
        } else {
            System.out.println("I'm the head");
            head = true;
        }
    
        
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new CommandLineInput()).execute(args));
    }
}
