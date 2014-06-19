package net.sognefest.data.collector.bitatamp;

import com.google.gson.Gson;
import com.pusher.client.Pusher;
import com.pusher.client.PusherOptions;
import com.pusher.client.channel.ChannelEventListener;
import com.pusher.client.connection.ConnectionEventListener;
import com.pusher.client.connection.ConnectionStateChange;

import java.util.Map;

/**
 * Use websocket to retrieve live trades from Bitstamp and add them to the database (through DBWriter).
 *
 * Based on example from pusher-java-client. At:
 * https://github.com/pusher/pusher-java-client/blob/master/src/main/java/com/pusher/client/example/PresenceChannelExampleApp.java
 */

public class BitstampLive implements ConnectionEventListener,
        ChannelEventListener {

    private final long startTime = System.currentTimeMillis();
    private final DBWriter dbWriter;


    public BitstampLive(DBWriter dbWriter) {

        this.dbWriter = dbWriter;
        String apiKey = "de504dc5763aeef9ff52";
        String channelName = "live_trades";
        String eventName = "trade";

        PusherOptions options = new PusherOptions().setEncrypted(true);
        Pusher pusher = new Pusher(apiKey, options);
        pusher.connect(this);

        pusher.subscribe(channelName, this, eventName);

        // Keep main thread asleep while we watch for events or application will terminate
        while(true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /* ConnectionEventListener implementation */

    @Override
    public void onConnectionStateChange(ConnectionStateChange change) {

        System.out.println(String.format(
                "[%d] Connection state changed from [%s] to [%s]",
                timestamp(), change.getPreviousState(), change.getCurrentState()));
    }

    @Override
    public void onError(String message, String code, Exception e) {

        System.out.println(String.format(
                "[%d] An error was received with message [%s], code [%s], exception [%s]",
                timestamp(), message, code, e));
    }

    /* ChannelEventListener implementation */

    @Override
    public void onEvent(String channelName, String eventName, String data) {

        //System.out.println(String.format(
        //        "[%d] Received event [%s] on channel [%s] with data [%s]",
        //        timestamp(), eventName, channelName, data));

        Gson gson = new Gson();
        @SuppressWarnings("unchecked")
        Map<String, String> jsonObject = gson.fromJson(data, Map.class);
        //System.out.println(jsonObject);

        Object[] jsonArray = jsonObject.values().toArray();

        int unixTimestamp = (int) (System.currentTimeMillis() / 1000);
        double price = (double) jsonArray[0];
        double amount = (double) jsonArray[1];
        long sourceId = (long) ((double) jsonArray[2]);

        dbWriter.javaNewTick(sourceId, unixTimestamp, price, amount);

    }

    @Override
    public void onSubscriptionSucceeded(String channelName) {

        System.out.println(String.format(
                "[%d] Subscription to channel [%s] succeeded",
                timestamp(), channelName));
    }

    private long timestamp() {
        return System.currentTimeMillis() - startTime;
    }
}
