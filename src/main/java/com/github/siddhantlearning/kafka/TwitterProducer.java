package com.github.siddhantlearning.kafka;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    String consumerKey = "5lxLTk00zw3VxlnQ1MuiEs6nU";
    String consumerSecret = "Kjluduevpr2m1vI26Gsiu8o82NXlRfBgqm0UehH7l3s8RGovbj";
    String token = "493200994-uyQutznr9LdhtQKgzbOf7YSwpXRDX9L6ZnvqwxEy";
    String secret = "06KzywQngpOJJkZOL01OWNLFFpE9EChyRMKotIvjD9eKW";

    public TwitterProducer(){}

    public static void main(String[] args) throws InterruptedException {
        new TwitterProducer().run();
    }

    public void run() throws InterruptedException {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        //Create a twitter client
        Client client = createTwitterClient(msgQueue);
        //Attempt to establish a connection
        client.connect();

        //create a kafka producer

        //loop to send tweets to kafka

        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e){
                e.printStackTrace();
                client.stop();
            }
            if(msg!=null){
                logger.info(msg);
            }
        }
        logger.info("End of application");
    }



    public Client createTwitterClient(BlockingQueue<String> msgQueue){


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("bitcoin");
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                ;                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();

        return hosebirdClient;

    }
}
