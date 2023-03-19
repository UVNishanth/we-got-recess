package edu.lunchbreaker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import edu.lunchbreaker.ZooLunchGrpc;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.io.*;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static io.grpc.Grpc.TRANSPORT_ATTR_REMOTE_ADDR;

public class Main {


    public static void main(String[] args) {
        System.exit(new CommandLine(new ServerCli()).execute(args));
    }

//    @Command(subcommands = {ServerCli.class})
//    static class Cli {
//    }

    @Command
    static class ServerCli implements Callable<Integer> {

        @Parameters(index = "0", description = "name")
        static String name;

        @Parameters(index = "1", description = "host:port listen on.")
        static
        String serverHostPort;
        static int serverPort;

        @Parameters(index = "2", description = "zookeeper server list")
        static String zookeeper_server_list;

        @Parameters(index = "3", description = "lunch path")
        static String lunchpath;

        static private Context.Key<SocketAddress> REMOTE_ADDR = Context.key("REMOTE_ADDR");


        @Override
        public Integer call() throws Exception {
            serverPort = Integer.valueOf(serverHostPort.split(":")[1]);
            System.out.printf("listening on %d\n", serverPort);
            var server = ServerBuilder.forPort(serverPort).intercept(new ServerInterceptor() {
                @Override
                public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> sc, Metadata h, ServerCallHandler<ReqT, RespT> next) {
                    var remote = sc.getAttributes().get(TRANSPORT_ATTR_REMOTE_ADDR);
                    return Contexts.interceptCall(Context.current().withValue(REMOTE_ADDR, remote), sc, h, next);
                }
            }).addService(new ZooLunchService()).build();
            server.start();
            server.awaitTermination();
            return 0;
        }

        static class ZooLunchService extends ZooLunchGrpc.ZooLunchImplBase {


            int lunchmatesCount;

            int waitForNextLeaderRun = 0;

            List<String> lunchmates;

            String leaderName;

            // TODO: DONE: Use Multimap instead of hashmap
            LinkedHashMap<Long, HashMap<String, String>> myLunchHistory = new LinkedHashMap<>();


            String hostname;
            String serverInfo = "";

            ZooKeeper zk;

            boolean amLeader = false;

            boolean amIAttending = false;

            String resto;


            boolean isItLunchtimeAlready = false;

            long currentLunchID;

            boolean skipNextLunch = false;

            boolean weReadyForRecess = false;


            ScheduledExecutorService scheduler
                    = Executors.newScheduledThreadPool(4);

            ScheduledFuture<?> future;


            /*
             * Watchers hangout spot
             * */

            Watcher readyforrecessWatcher = e -> {
                if (e.getType() == Watcher.Event.EventType.NodeCreated) {
                    //new Thread(this::runForLeader).start();
                    weReadyForRecess = true;
                    addMeToLunchRoster();
                } else if (e.getType() == Watcher.Event.EventType.NodeDeleted) {
                    System.out.println("/lunch/readyforrecess died. Registering again.....");
                    weReadyForRecess = false;

                }
                regForLunch();
            };

            Watcher getLunchChildrenWatcher = e -> {
                if (e.getType() == Watcher.Event.EventType.NodeDeleted) {
                    getLunchChildren();
                } else if (e.getType() == Watcher.Event.EventType.NodeCreated) {
                    // TODO: check if /lunchtime already present if so, then show below prompt
                    System.out.println("Cannot create new node in /lunch when lunchtime is in session");
                }
            };
            Watcher lunchtimeWatcher = e -> {
                if (e.getType() == Watcher.Event.EventType.NodeCreated) {
                    isItLunchtimeAlready = true;
                    System.out.println("It's lunchtime already!!!");
                    getLunchtimeInfo();
                    if (amLeader) {
                        getLunchChildren();
                    }
                    checkLunchtime();
                } else if (e.getType() == Watcher.Event.EventType.NodeDeleted) {
                    System.out.println("/lunchtime deleted ");
                    String path = lunchpath + "/krelboyne-" + name + "@" + serverPort;
                    isItLunchtimeAlready = false;
                    try {
                        zk.delete(path, -1);
                    } catch (KeeperException ex) {
                        if (ex.code() == KeeperException.Code.NONODE) {
                            System.out.println("Node does not exist to be deleted");
                        } else if (ex.code() == KeeperException.Code.BADVERSION) {
                            System.out.println("Version number do not match");
                        } else if (ex.code() == KeeperException.Code.NOTEMPTY) {
                            System.out.println("Cannot delete coz node has children");
                        }
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }

                    //wereadyforrecess = false;
                    if (amLeader) {
                        deleteLeader();
                    }

                    System.out.println("Lunch over. Omnomnom\n\n\n\n\n");
                    System.out.println("wait for leader: " + waitForNextLeaderRun);
                    regForLunch();
                }
            };

            Watcher leaderExistsWatcher = e -> {
                if (e.getType() == Watcher.Event.EventType.NodeDeleted) {
                    System.out.println("Leader dead");
                    scheduler.schedule(this::runForLeader, waitForNextLeaderRun, TimeUnit.SECONDS);

                }
            };


            /*
             * Only Callbacks allowed past this point
             * */ AsyncCallback.StringCallback newEmployeeCallBack = (rc, path, ctx, name) -> {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS -> addMeCozMNew();
                    case NODEEXISTS -> System.out.println("I am already registered as an employee");
                    case OK -> {
                        System.out.println("I am registered!!");
                        System.out.println("Let's get added to the Lunch Roster....");
                        regForLunch();
                    }
                }
            };


            AsyncCallback.StringCallback leaderCreateCallback = (rc, path, ctx, name) -> {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS -> checkLeader();
                    case OK -> {
                        amLeader = true;
                        // as i am the leader, i need not put a watcher on the leader so no leaderExists() called
                        System.out.println("I'm the leader");
                        System.out.println("my amLeader val: " + amLeader);
                    }
                    case NODEEXISTS -> {
                        if (leaderName.equals(serverInfo)) {
                            return;
                        }
                        amLeader = false;
                        //Decrement wait
                        if (waitForNextLeaderRun > 0) {
                            waitForNextLeaderRun--;
                        }

                        System.out.println("I'm not the leader");
                        System.out.println("my amLeader val: " + amLeader);
                    }
                    default -> System.out.println("Something went wrong while creating leader");
                }
                leaderExists();
                checkLeader();
                checkLunchtime();
            };

            AsyncCallback.StatCallback leaderExistsCallback = (rc, path, ctx, stat) -> {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS -> leaderExists();
//                    case OK -> {
//                        // run for leader only when znode is gone
//                        if (stat == null)
//                            //runForLeader();
//                            new Thread(this::runForLeader).start();
//                    }
//                    default -> checkLeader();
                }
            };

            AsyncCallback.DataCallback leaderCheckCallback = (rc, path, ctx, data, stat) -> {
                if (Objects.requireNonNull(KeeperException.Code.get(rc)) == KeeperException.Code.CONNECTIONLOSS) {
                    checkLeader();
                }
                leaderName = new String(data, StandardCharsets.UTF_8);

                HashMap<String, String> lunchInfo = myLunchHistory.get(currentLunchID);
                lunchInfo.put("leader", leaderName);
                lunchInfo.put("rc", amLeader ? Integer.toString(0) : Integer.toString(1));
                myLunchHistory.put(currentLunchID, lunchInfo);

                //new Thread(this::runForLeader).start();


            };

            AsyncCallback.ChildrenCallback getLunchChildrenCallback = (rc, path, ctx, children) -> {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS -> getLunchChildren();
                    /*Will not happen unless the client acts as an adversary because the leader will always
                     * have its name in the lunch roster*/
                    case NONODE ->
                            System.out.println("No LunchChildren nodes present. " + "Someone destroyed the sacred texts. Alert the guards!!");
                    case OK -> {
                        System.out.println("Leader (aka me) got info of my lunchmates");
                        // As lunch children will also have employees znode, we need to filter that out
                        children.removeIf(znode -> !znode.startsWith("krelboyne-"));
                        lunchmates = children;
                        lunchmatesCount = lunchmates.size();
                        waitForNextLeaderRun = lunchmates.size();
                        resto = "Bistro-" + serverPort + " on the " + hostname + " boulevard";
                        // store these info to hashmap
                        HashMap<String, String> lunchInfo = myLunchHistory.get(currentLunchID);
                        lunchInfo.put("restaurant", resto);
                        lunchInfo.put("attendees", lunchmates.stream().map(Object::toString).collect(Collectors.joining(", ")));
                        System.out.println("Taking my homies:");
                        lunchmates.forEach(System.out::println);
                        System.out.println("to" + resto);
                        myLunchHistory.put(currentLunchID, lunchInfo);
                        new Thread(this::persistData).start();


                    }
                    default -> System.out.println("getLunchChildren failed");
                }
            };

            AsyncCallback.StringCallback addToLunchRosterCallback = (rc, path, ctx, name) -> {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS -> {
                        if (!isItLunchtimeAlready) {
                            addMeToLunchRoster();
                        } else {
                            System.out.println("Missed the chance to register for lunch :(");
                        }
                    }
                    case OK -> {
                        System.out.println("Am registered for Lunch!!");
                        amIAttending = true;
                        System.out.println("Let's run for leader");
                    }
                }

                scheduler.schedule(this::runForLeader, waitForNextLeaderRun, TimeUnit.SECONDS);


            };

            AsyncCallback.StringCallback createLunchCallback = (rc, path, ctx, name) -> {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS -> {
                        System.out.println("Connection loss. Trying again to create /lunch node");
                        createLunch();
                    }
                    case NODEEXISTS -> {
                        System.out.println("/lunch node already exists");
                    }

                }
                createEmployee();
            };

            AsyncCallback.StringCallback createEmployeeCallback = (rc, path, ctx, name) -> {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS -> {
                        System.out.println("Connection loss. Trying again to create /lunch node");
                        createEmployee();
                    }
                    case NODEEXISTS -> {
                        System.out.println("/lunch/employee node already exists");
                    }

                }
                addMeCozMNew();
            };

            AsyncCallback.StatCallback getLunchtimeInfoCallback = (rc, path, ctx, stat) -> {
                System.out.println("am in lunchtimecallback");
                currentLunchID = stat.getCzxid();
                System.out.println("lunchtime id: " + currentLunchID);
                myLunchHistory.put(currentLunchID, new HashMap<String, String>());
                // TODO: way to trigger persistData when myLunchHistory changes
                //  i.e. attaching observer/listener?
                new Thread(this::persistData).start();
                //persistData();

            };


            /*
             * Hiding my complexities here
             * */

            void persistData() {
                System.out.println("Persisting LunchHistory now....");
                try (Writer writer = new FileWriter("myLunchHistory" + serverPort + ".json")) {
                    JSONObject myLunchHistoryInJson = new JSONObject(myLunchHistory);
                    writer.write(myLunchHistoryInJson.toJSONString());

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            }

            void addMeCozMNew() {
                zk.create(lunchpath + "/employee/krelboyne-" + name + "@" + serverPort, serverInfo.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, newEmployeeCallBack, null);

            }

            void regForLunch() {
                if (skipNextLunch) {
                    skipNextLunch = false;
                    return;
                }
                System.out.println("Adding watcher to /readylunch");
                try {
                    zk.exists(lunchpath + "/readyforrecess", readyforrecessWatcher);
                } catch (KeeperException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            void getLunchChildren() {
                zk.getChildren(lunchpath, getLunchChildrenWatcher, getLunchChildrenCallback, null);
            }


            void addMeToLunchRoster() {
                zk.create(lunchpath + "/krelboyne-" + name + "@" + serverPort, serverInfo.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, addToLunchRosterCallback, null);
            }

            void getLunchtimeInfo() {
                zk.exists(lunchpath + "lunchtime", false, getLunchtimeInfoCallback, null);
            }


            void leaderExists() {
                zk.exists(lunchpath + "/leader", leaderExistsWatcher, leaderExistsCallback, null);
            }

            void checkLeader() {
                zk.getData(lunchpath + "/leader", false, leaderCheckCallback, null);
            }


            void runForLeader() {
                if (!weReadyForRecess) {
                    return;
                }
                if (isItLunchtimeAlready) {
                    leaderExists();
                    return;
                }
                zk.create(lunchpath + "/leader", serverInfo.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, leaderCreateCallback, null);
            }

            void checkLunchtime() {
                try {
                    zk.exists(lunchpath + "/lunchtime", lunchtimeWatcher);
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            void deleteLeader() {
                try {
                    zk.delete(lunchpath + "/leader", -1);
                    //leaderExists();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (KeeperException e) {
                    if (e.code() == KeeperException.Code.CONNECTIONLOSS) {
                        deleteLeader();
                    } else {
                        throw new RuntimeException(e);
                    }
                }

            }

            void readLunchHistoryFromFile() {
                try {
                    // convert JSON file to ma
                    JSONObject myLunchHistoryInJson = (JSONObject) new JSONParser().parse(new FileReader("myLunchHistory" + serverPort + ".json"));

                    myLunchHistory = new ObjectMapper().readValue(myLunchHistoryInJson.toJSONString(), LinkedHashMap.class);

                    System.out.println("myLunchHistory set from file");

                } catch (FileNotFoundException e) {
                    System.out.println("myLunchHistory file not yet created");
                } catch (IOException | ParseException e) {
                    throw new RuntimeException(e);
                }
            }

            void createLunch() {
                zk.create(lunchpath, serverInfo.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                        createLunchCallback, null);
            }

            void createEmployee() {
                zk.create(lunchpath + "/employee", serverInfo.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                        createEmployeeCallback, null);
            }

            public ZooLunchService() throws IOException {
                zk = new ZooKeeper(zookeeper_server_list, 10000, (e) -> {
                    System.out.println(e);
                });

                try {
                    InetAddress addr;
                    addr = InetAddress.getLocalHost();
                    hostname = addr.getHostName();
                } catch (UnknownHostException ex) {
                    System.out.println("Hostname can not be resolved");
                    return;
                }

                serverInfo = hostname + ":" + serverPort;

                /*
                 * Reading History from file
                 * */

                readLunchHistoryFromFile();


                /*
                 * createLunch starts the domino
                 * */


                createLunch();


            }

            @Override
            public void goingToLunch(Grpc.GoingToLunchRequest request, StreamObserver<edu.lunchbreaker.Grpc.GoingToLunchResponse> responseObserver) {

                // to get latest lunch id
                ArrayList<Long> attendedLunches = new ArrayList<Long>(myLunchHistory.keySet());

                Long latestLunchID = attendedLunches.get(attendedLunches.size() - 1);


                HashMap<String, String> latestLunchInfo = myLunchHistory.get(latestLunchID);

                int rc = Integer.parseInt(latestLunchInfo.get("rc"));
                String restaurant = "";
                List<String> attendees = Lists.newArrayList("");

                if (rc == 0) {
                    restaurant = latestLunchInfo.get("restaurant");
                    attendees = Arrays.asList(latestLunchInfo.get("attendees").split(","));
                }
                responseObserver.onNext(edu.lunchbreaker.Grpc.GoingToLunchResponse.newBuilder().setRc(rc).setRestaurant(restaurant).addAllAttendees(attendees).build());
                responseObserver.onCompleted();
            }

            @Override
            public void lunchesAttended(edu.lunchbreaker.Grpc.LunchesAttendedRequest request, StreamObserver<edu.lunchbreaker.Grpc.LunchesAttendedResponse> responseObserver) {
                ArrayList<Long> attendedLunches = new ArrayList<Long>(myLunchHistory.keySet());
                responseObserver.onNext(edu.lunchbreaker.Grpc.LunchesAttendedResponse.newBuilder().addAllZxids(attendedLunches).build());
                responseObserver.onCompleted();

            }

            @Override
            public void getLunch(edu.lunchbreaker.Grpc.GetLunchRequest request, StreamObserver<edu.lunchbreaker.Grpc.GetLunchResponse> responseObserver) {

                long zxid = request.getZxid();

                int rc = 2;
                String leader = "";
                String restaurant = "";
                List<String> attendees = Lists.newArrayList("");

                if (myLunchHistory.containsKey(zxid)) {

                    HashMap<String, String> lunchInfo = myLunchHistory.get(zxid);
                    rc = Integer.parseInt(lunchInfo.get("rc"));
                    leader = lunchInfo.get("leader");

                    if (rc == 0) {
                        restaurant = lunchInfo.get("restaurant");
                        attendees = Arrays.asList(lunchInfo.get("attendees").split(","));
                    }
                }
                responseObserver.onNext(edu.lunchbreaker.Grpc.GetLunchResponse.newBuilder().setRc(rc).setLeader(leader).setRestaurant(restaurant).addAllAttendees(attendees).build());
                responseObserver.onCompleted();


            }

            @Override
            public void skipLunch(edu.lunchbreaker.Grpc.SkipRequest request, StreamObserver<edu.lunchbreaker.Grpc.SkipResponse> responseObserver) {
                responseObserver.onNext(edu.lunchbreaker.Grpc.SkipResponse.newBuilder().build());
                responseObserver.onCompleted();
                skipNextLunch = true;

            }

            @Override
            public void exitZoo(edu.lunchbreaker.Grpc.ExitRequest request, StreamObserver<edu.lunchbreaker.Grpc.ExitResponse> responseObserver) {
                responseObserver.onNext(edu.lunchbreaker.Grpc.ExitResponse.newBuilder().build());
                responseObserver.onCompleted();
                System.out.println("Exit request from " + REMOTE_ADDR.get());
                System.exit(0);
            }

        }

    }


}