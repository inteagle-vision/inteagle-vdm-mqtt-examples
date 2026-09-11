package com.inteagle.examples.vdm;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.inteagle.vdm.mqtt.sdk.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/** Run one MQTT alarm RPC. Default: read-only capabilities. Run from java/. */
public final class AlarmRpc {
  public static void main(String[] args) throws Exception {
    String name="capabilities", file=null;
    boolean printOnly=false, selectedCase=false;
    long seconds=0;
    for (int i=0;i<args.length;i++) {
      switch(args[i]) {
        case "--print-only" -> printOnly=true;
        case "--case" -> { name=args[++i]; selectedCase=true; }
        case "--request" -> file=args[++i];
        case "--listen" -> seconds=Long.parseLong(args[++i]);
        default -> throw new IllegalArgumentException("Unknown argument: "+args[i]);
      }
    }
    if(seconds<0) throw new IllegalArgumentException("--listen must be nonnegative");
    if(selectedCase && file!=null) throw new IllegalArgumentException("Choose --case or --request");
    var mapper=new ObjectMapper();
    var format=PayloadFormat.parse(System.getenv().getOrDefault("VDM_PAYLOAD_FORMAT","protobuf"));
    String method=null;
    Map<String,Object> params=null;
    if(file!=null) {
      var request=mapper.readTree(Path.of(file).toFile());
      method=request.get("method").asText();params=mapper.convertValue(request.get("params"),new TypeReference<>(){});
    } else {
      for(var item:mapper.readTree(Path.of("../examples/alarms/requests.json").toFile())) {
        if(item.get("name").asText().equals(name)) {method=item.get("method").asText();params=mapper.convertValue(item.get(format.toString()),new TypeReference<>(){});break;}
      }
      if(method==null) throw new IllegalArgumentException("Unknown case: "+name);
    }
    int reqId=ThreadLocalRandom.current().nextInt(1,Integer.MAX_VALUE);
    var encoded=new VdmCodec(format).encodeRpcRequest(method,params,reqId);
    System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(Map.of("method",method,"params",params)));
    System.out.printf("format=%s bytes=%d reqId=%d%n",format,encoded.payload().length,reqId);
    if(printOnly)return;
    String host=System.getenv("MQTT_HOST"),device=System.getenv("VDM_DEVICE_ID");
    if(host==null || device==null)throw new IllegalArgumentException("Set MQTT_HOST and VDM_DEVICE_ID");
    try(var client=new VdmMqttClient(new VdmMqttClient.Config(host,Integer.parseInt(System.getenv().getOrDefault("MQTT_PORT","1883")),VdmTopics.forDevice(device),format,System.getenv("MQTT_USERNAME"),System.getenv("MQTT_PASSWORD"),null,1,Duration.ofSeconds(10)),
      message->{if(message.suffix().equals("3A") || message.suffix().equals("event"))System.out.println(message.suffix()+" "+message.data());},Throwable::printStackTrace)) {
      client.start();
      // Blocking RPC stays on the main thread, never on the MQTT callback.
      var response=client.call(method,params,reqId,Duration.ofSeconds(30),false);
      System.out.println("RESPONSE "+response.data());
      Thread.sleep(seconds*1000);
    }
  }
}
