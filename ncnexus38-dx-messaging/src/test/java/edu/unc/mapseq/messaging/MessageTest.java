package edu.unc.mapseq.messaging;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;

public class MessageTest {

    @Test
    public void testQueue() {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(String.format("nio://%s:61616", "biodev2.its.unc.edu"));

        Connection connection = null;
        Session session = null;
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("queue/ncnexus.dx");
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            String format = "{\"entities\":[{\"entityType\":\"Sample\",\"id\":\"%d\",\"attributes\":[{\"name\":\"GATKDepthOfCoverage.interval_list.version\",\"value\":\"%d\"},{\"name\":\"SAMToolsView.dx.id\",\"value\":\"%d\"}]},{\"entityType\":\"WorkflowRun\",\"name\":\"%s\"}]}";

            producer.send(session.createTextMessage(String.format(format, 52422, 7, 5, "NCG_00009_V7_Dx5")));

        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            try {
                session.close();
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

    }

}
