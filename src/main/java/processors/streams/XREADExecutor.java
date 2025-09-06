package processors.streams;

import db.DataStore;
import models.DataStoreValue;
import models.RespCommand;
import processors.CommandExecutor;
import utility.RespUtility;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;

public class XREADExecutor implements CommandExecutor {

  @Override
  public String execute(RespCommand cmd) {
    List<String> args = cmd.getArgs();
    String key = args.get(1);
    String streamId = args.get(2);
    DataStoreValue value = DataStore.get(key);
    if(value == null) {
      return "*-1\r\n";
    }
    ConcurrentNavigableMap<String, Map<String, String>> streamData = value.getAsStream();
    return encodeResponse(streamData.tailMap(streamId, false), key);
  }

  private String encodeResponse(ConcurrentNavigableMap<String, Map<String, String>> output, String key) {
    StringBuilder op = new StringBuilder();
    op.append("*1\r\n*2\r\n");
    op.append(RespUtility.serializeResponse(key));
    op.append(RespUtility.serializeStreamResponse(output));
    System.out.println("oppppp"+ op);
    return op.toString();
  }
}
