package com.bcbsfl.mail;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bcbsfl.es.Utils;

/**
 * Sends standard emails to Conductor interested parties
 */
public class EmailSender {
    private static final Logger logger = LoggerFactory.getLogger(EmailSender.class);
    private Session mailSession = null;
	private String smtpHost = null;
	private String from = null;
	private String replyTo = null;
	private String to = null;
	private String environment = null;
	
	public EmailSender() {
		this.smtpHost = getTrimmedString(Utils.getProperty("email.smtp.host"));
		if(this.smtpHost != null) {
			this.from = getTrimmedString(Utils.getProperty("email.address.from"));
			this.replyTo = getTrimmedString(Utils.getProperty("email.address.replyto"));
			this.to = getTrimmedString(Utils.getProperty("email.address.to"));
			Properties props = System.getProperties();
			props.put("mail.smtp.host", this.smtpHost);
			this.mailSession = Session.getInstance(props, null);
			this.environment = Utils.getProperty("env");
		}
	}
	
	public void sendDuplicatesEmail(String doctype, Map<String, List<String>> duplicateInfo, int totalDups, int maxDups) {
		String message = "Found " + totalDups + " " + doctype + "s that had more than one ElasticSearch document";
		try {
			StringBuffer sb = new StringBuffer("<html><body><table><div style='color:black;font-family:Arial,Helvetica,sans-serif;font-size:11px;'>");
			sb.append("<div style='font-weight:bold;font-size:12px;font-style:italic;margin-bottom:10px;'>");
			sb.append("Note that the duplicate ElasticSearch documents were deleted so this is just a notification");
			if(totalDups > maxDups) {
				sb.append(". Showing the first " + maxDups + " duplicates.");
			}
			sb.append("</div>");
			for(String id: duplicateInfo.keySet()) {
				sb.append(doctype + "_id: " + "<strong>" + id + "</strong>" + "<br/>");
				for(String event : duplicateInfo.get(id)) {
					sb.append("&nbsp;&nbsp;&nbsp;" + event + "<br/>");
				}
			}
			sb.append("</div></body></html>");
			sendEmail(message, sb.toString());
		} catch(Exception e) {
			logger.error("Error sending email for message '" + message + "'. Got this exception", e);
		}
	}
	
	/**
	 * Send an email in html format 
	 * @param message a message to display in the email describing the context
	 * @param body the body of the email
	 */
	public void sendEmail(String message, String body) {
		if(this.smtpHost == null) {
			return;
		}
		try {
			MimeMessage msg = getMessage();
			msg.setSubject("[" + this.environment + "] Conductor Reconciliation Error: " + message, "UTF-8");
			msg.setText(body, "utf-8", "html");
			Transport.send(msg);
		} catch(Exception e) {
			logger.error("Error sending email for message '" + message + "'. Got this exception", e);
		}
	}

	private MimeMessage getMessage() throws Exception {
		MimeMessage msg = new MimeMessage(this.mailSession);
		msg.addHeader("format", "flowed");
		msg.addHeader("Content-Transfer-Encoding", "8bit");
		if(StringUtils.isNotEmpty(this.from)) {
			msg.setFrom(new InternetAddress(this.from));
		}
		if(StringUtils.isNotEmpty(this.replyTo)) {
			msg.setReplyTo(InternetAddress.parse(this.replyTo, false));
		}
		msg.setSentDate(new Date());
		String recipients = this.to;
		msg.addRecipients(Message.RecipientType.TO, InternetAddress.parse(recipients));
		return msg;
	}

	public String getTrimmedString(String s) {
		return s == null ? null : s.trim();
	}

	public String getSmtpHost() {
		return smtpHost;
	}
	public void setSmtpHost(String smtpHost) {
		this.smtpHost = smtpHost;
	}
	public String getFrom() {
		return from;
	}
	public void setFrom(String from) {
		this.from = from;
	}
	public String getReplyTo() {
		return replyTo;
	}
	public void setReplyTo(String replyTo) {
		this.replyTo = replyTo;
	}
	public String getTo() {
		return to;
	}
	public void setTo(String to) {
		this.to = to;
	}
}
