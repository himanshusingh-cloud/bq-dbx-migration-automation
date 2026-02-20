package com.analytics.orchestrator.report;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailService;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder;
import com.amazonaws.services.simpleemail.model.RawMessage;
import com.amazonaws.services.simpleemail.model.SendRawEmailRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.mail.Message;
import javax.mail.Session;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Properties;

/**
 * Sends Allure report via AWS SES. Uses IAM role (EC2/ECS) or ~/.aws/credentials when running locally.
 */
@Service
public class ReportEmailService {

    private static final Logger log = LoggerFactory.getLogger(ReportEmailService.class);

    private final AmazonSimpleEmailService sesClient;

    @Value("${orchestrator.report.ses.from-email}")
    private String fromEmail;

    @Value("${orchestrator.report.subject:Analytics API Test Report}")
    private String subjectPrefix;

    public ReportEmailService() {
        this.sesClient = AmazonSimpleEmailServiceClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();
    }

    /**
     * Send report zip to the given email address. Uses AWS SES (IAM or ~/.aws/credentials).
     */
    public void sendReport(String toEmail, java.nio.file.Path reportZipPath, String executionId, int passed, int failed) {
        try {
            String subject = subjectPrefix + " - " + executionId + " (Passed: " + passed + ", Failed: " + failed + ")";
            String body = "Analytics API test execution completed.\n\nExecution ID: " + executionId
                    + "\nPassed: " + passed + "\nFailed: " + failed
                    + "\n\nPlease find the Allure report attached (zip). Extract and open index.html to view.";

            Session session = Session.getDefaultInstance(new Properties());
            MimeMessage mimeMessage = new MimeMessage(session);
            mimeMessage.setFrom(new InternetAddress(fromEmail));
            mimeMessage.setRecipients(Message.RecipientType.TO, InternetAddress.parse(toEmail));
            mimeMessage.setSubject(subject);

            MimeMultipart multipart = new MimeMultipart();
            MimeBodyPart bodyPart = new MimeBodyPart();
            bodyPart.setText(body);
            multipart.addBodyPart(bodyPart);

            MimeBodyPart attachPart = new MimeBodyPart();
            attachPart.attachFile(reportZipPath.toFile());
            attachPart.setFileName("allure-report.zip");
            multipart.addBodyPart(attachPart);

            mimeMessage.setContent(multipart);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            mimeMessage.writeTo(out);
            byte[] raw = out.toByteArray();

            SendRawEmailRequest request = new SendRawEmailRequest(new RawMessage(ByteBuffer.wrap(raw)));
            sesClient.sendRawEmail(request);
            log.info("Report sent to {} via AWS SES", toEmail);
        } catch (Exception e) {
            log.error("Failed to send report to {}: {}", toEmail, e.getMessage(), e);
            throw new RuntimeException("Failed to send report email: " + e.getMessage());
        }
    }
}
