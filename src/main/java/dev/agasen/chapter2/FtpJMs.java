package dev.agasen.chapter2;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.camel.*;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.language.XPath;

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
//        context.addRoutes(new CBRTestRouteWithFilter());
        context.addRoutes(new CBRTestRouteWithFilterAndMulticast());


        context.start();
        Thread.sleep(100000);
        context.stop();
    }

    // using RecipientList
    static class RecipientListRoute extends RouteBuilder {

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

            from("jms:xmlOrders")
                    .bean(RecipientsBean.class)
                    .log("Received XML order: ${header.CamelFileName}");

            // adding pipeline to prod and accounting
            from("jms:production")
                    .log("Production received ${header.CamelFileName}");

            from("jms:accounting")
                    .log("Accounting received ${header.CamelFileName}");


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

    static class RecipientsBean {

        @RecipientList
        public String[] recipients(@XPath("/order/@customer") String customer) {
            if (customer.equals("honda")) {
                return new String[] { "jms:accounting", "jms:production" };
            } else {
                return new String[] { "jms:accounting" };
            }
        }
    }


    // using multicasting
    static class CBRTestRouteWithFilterAndMulticast extends RouteBuilder {

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

            from("jms:xmlOrders")
//                    .filter(xpath("/order[not(@test)]"))
                    .log("Received XML order: ${header.CamelFileName}")
                    // multicasting
                    .multicast()
                    // for parallel processing, by default it is sequential
//                    .parallelProcessing()
                    .stopOnException()
                    .to("jms:production", "jms:accounting", "file:data/outbox/xml");

            // adding pipeline to prod and accounting
            from("jms:production")
                    // adding this to test if stopOnException works
//                    .throwException(Exception.class, "I failed!!!")
                    .log("Production received ${header.CamelFileName}");

            from("jms:accounting")
                    .log("Accounting received ${header.CamelFileName}");


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
