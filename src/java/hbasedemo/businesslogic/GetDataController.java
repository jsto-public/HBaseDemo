package hbasedemo.businesslogic;

import javax.ws.rs.core.MediaType;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


// REST method to retrieve interval data for given meterId
@RestController
public class GetDataController {
   @RequestMapping(value = "/GetData", method = RequestMethod.GET)
    public String GetData(@RequestParam(value = "meterId", defaultValue = "1") String meterId) {
        StringBuilder stringBuilder = new StringBuilder();
        
        IntervalDataSet intervalDataSet = null;        
        try {
            intervalDataSet = DataLayer.readIntervalDataSet(meterId);
        }
        catch (Exception e) {};
        
        // having trouble getting all the Jackson dependencies to work to auto-serialize
        // to JSON; for the moment, just build the JSON myself.

        stringBuilder.append("{\"data\": [\r\n");

        if (intervalDataSet != null && intervalDataSet.intervalData != null) {
            int i = 0;
            for (IntervalData intervalData : intervalDataSet.intervalData) {
                if (i > 0) {
                    stringBuilder.append(",\r\n");
                }
                
                long BASE = 1388563200000L;
                long ticksFromBase = (intervalData.epochTime - BASE);
                long epochTimeAdj = BASE + ticksFromBase * 1000 * 60;

                stringBuilder.append("[");
                stringBuilder.append(epochTimeAdj);
                stringBuilder.append(",");
                
                stringBuilder.append(intervalData.value);
                stringBuilder.append("]");                
                i++;
            }            
        }

        stringBuilder.append("]}");
        
        return stringBuilder.toString();
    }
}
