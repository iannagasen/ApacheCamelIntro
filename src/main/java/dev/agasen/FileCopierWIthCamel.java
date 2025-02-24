package dev.agasen;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;

public class FileCopierWIthCamel {

    public static void main(String args[]) throws Exception {
        CamelContext context = new DefaultCamelContext();
        context.addRoutes(new Route());
        context.start();
        Thread.sleep(10000);
        context.stop();

    }

    static class Route extends RouteBuilder {

        // in just 2 lines we create a file polling
        @Override
        public void configure() throws Exception {
            from("file:data/inbox?noop=true")
                .to("file:data/outbox");
        }
    }
}
