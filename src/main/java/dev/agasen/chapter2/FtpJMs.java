package dev.agasen.chapter2;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.impl.DefaultCamelContext;

import javax.jms.ConnectionFactory;

public class FtpJMs {

    static String ftpuser = System.getenv("wsluser");
    static String ftppass = System.getenv("wslpass");

    public static void main(String[] args) throws Exception {
        // Create a context
        CamelContext context = new DefaultCamelContext();

        // Connect to embedded ActiveMQ JMS broker : NOTE - embedded not working properly
//        ConnectionFactory factory = new ActiveMQConnectionFactory("vm://broker");

        // use this docker image for now
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        factory.setUserName("artemis");
        factory.setPassword("artemis");
        context.addComponent("jms", JmsComponent.jmsComponentAutoAcknowledge(factory));

        // add route to the CamelContext
//        context.addRoutes(new DefaultRoute());
//        context.addRoutes(new CBRTestRoute());
//        context.addRoutes(new RoutingAfterCBR());
        context.addRoutes(new CBRTestRouteWithFilter());

        context.start();
        Thread.sleep(100000);
        context.stop();
    }

    static class CBRTestRouteWithFilter extends RouteBuilder {

        @Override
        public void configure() throws Exception {


            from("ftp://172.31.245.38/orders?username=" + ftpuser + "&password=" + ftppass + "&move=processed/${file:name}")
                    .to("jms:incomingOrders");

            from("jms:incomingOrders")
                    .choice()
                    .when(header("CamelFileName").endsWith(".xml"))
                    .to("jms:xmlOrders")
                    .when(header("CamelFileNAme").endsWith(".csv"))
                    .to("jms:csvOrders")
                    .otherwise()
                    .to("jms:badOrders");

            // FILTER OUT: xml with test attribute(regardless of the attr value)
            from("jms:xmlOrders")
                    .filter(xpath("/order[not(@test)]"))
                    .log("Received XML order: ${header.CamelFileName}")
                    .to("file:data/outbox/xml");


            from("jms:csvOrders")
                    .log("Received CSV order: ${header.CamelFileName}")
                    .to("file:data/outbox/csv");


            from("jms:badOrders")
                    .log("Received a bad order: ${header.CamelFileName}")
                    .to("file:data/outbox/badOrders");

            from("jms:continuedProcessing")
                    .log("order will continue processing: ${header.CamelFileName}");
        }
    }


    static class CBRTestRoute extends RouteBuilder {

        @Override
        public void configure() throws Exception {
            from("ftp://172.31.245.38/orders?username=ftpuser&password=ftpuser&move=processed/${file:name}")
                    .to("jms:incomingOrders");

            from("jms:incomingOrders")
                    .choice()
                    .when(header("CamelFileName").endsWith(".xml")).to("jms:xmlOrders")
                    .when(header("CamelFileNAme").endsWith(".csv")).to("jms:csvOrders")
                    .otherwise().to("jms:badOrders");

            from("jms:xmlOrders")
                    .log("Received XML order: ${header.CamelFileName}")
                    .to("file:data/outbox/xml");


            from("jms:csvOrders")
                    .log("Received CSV order: ${header.CamelFileName}")
                    .to("file:data/outbox/csv");


            from("jms:badOrders")
                    .log("Received a bad order: ${header.CamelFileName}")
                    .to("file:data/outbox/badOrders");


        }
    }

    static class RoutingAfterCBR extends RouteBuilder {

        @Override
        public void configure() throws Exception {
            from("ftp://172.31.245.38/orders?username=ftpuser&password=ftpuser&move=processed/${file:name}")
                    .to("jms:incomingOrders");

            from("jms:incomingOrders")
                    .choice()
                    .when(header("CamelFileName").endsWith(".xml"))
                        .to("jms:xmlOrders")
                    .when(header("CamelFileNAme").endsWith(".csv"))
                        .to("jms:csvOrders")
                    .otherwise()
                        .to("jms:badOrders").stop()
                    .end()
                    .to("jms:continuedProcessing");

            from("jms:xmlOrders")
                    .log("Received XML order: ${header.CamelFileName}")
                    .to("file:data/outbox/xml");


            from("jms:csvOrders")
                    .log("Received CSV order: ${header.CamelFileName}")
                    .to("file:data/outbox/csv");


            from("jms:badOrders")
                    .log("Received a bad order: ${header.CamelFileName}")
                    .to("file:data/outbox/badOrders");

            from("jms:continuedProcessing")
                    .log("order will continue processing: ${header.CamelFileName}");
        }
    }

    static class DefaultRoute extends RouteBuilder {

        @Override
        public void configure() throws Exception {
            from("ftp://172.31.245.38/orders?username=ftpuser&password=ftpuser&move=processed/${file:name}")
//                        .log("Downloading file: ${file:name}")
//                        .to("file:data/outbox");
                    .process(new MyProcessor())
                    .to("jms:queue:incomingOrders");
        }
    }

    static class MyProcessor implements Processor {

        @Override
        public void process(Exchange exchange) throws Exception {
            System.out.println("We just downloaded " + exchange.getIn().getHeader("CamelFileName"));
        }
    }
}
