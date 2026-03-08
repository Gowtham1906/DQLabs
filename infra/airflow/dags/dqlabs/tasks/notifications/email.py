import smtplib
import ssl
import base64
import boto3
import requests
import json
from sendgrid import SendGridAPIClient
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path
from sendgrid.helpers.mail import (
    Mail,
    Attachment,
    FileContent,
    FileName,
    FileType,
    Disposition,
    ContentId,
)
from dqlabs.utils.notifications import get_summary_template

from dqlabs.utils.notifications import (
    update_notification_status,
    get_dqlabs_default_images,
    get_client_details,
)
from dqlabs.app_helper.dq_helper import get_client_domain
from dqlabs.app_constants.dq_constants import (
    SUCCESS,
    FAILED,
    SEND_GRID,
    GMAIL,
    OUTLOOK,
    AWS_SES,
    PROOFPOINT,
)
from dqlabs.app_helper.log_helper import log_error
from dqlabs.app_helper.crypto_helper import decrypt
from dqlabs.utils.catalog_update import (
    check_vault_enabled,
    extract_vault_credentials
)

def send_mail_notification(notification_info: dict, config: dict={}):
    mailserver = None
    try:
        channel_config = notification_info.get("channel_config", None)
        is_report = notification_info.get("is_report", False)
        is_workflow = notification_info.get("is_workflow", False)
        is_summary_notification = notification_info.get("is_summary", False)
        if not channel_config:
            return
        smtp_type = channel_config.get("smtp_type", GMAIL)
        aws_ses_auth_type = channel_config.get("aws_ses_auth_type", "secret_key")
        proofpoint_integration_type = channel_config.get("proofpoint_integration_type", "")
        receivers_email = notification_info.get("content", {}).get("to", [])
        if check_vault_enabled(channel_config):
            configuration = extract_vault_credentials(config, channel_config)
            if configuration:
                channel_config.update(**configuration)
                if 'user' in configuration:
                    channel_config['user_name'] = configuration['user']         
        if smtp_type == SEND_GRID:
            # sender_email = channel_config.get("sender_email") # not required
            api_key = decrypt(channel_config.get("apiKey"))
            success = send_via_sendgrid(notification_info, api_key, receivers_email, is_summary_notification)

        if smtp_type == OUTLOOK:
            integration_type = channel_config.get("integration_type", "")
            if integration_type == "ms_graph":
                success = send_via_outlook(notification_info, receivers_email, is_summary_notification)
            else:
                sender_email = decrypt(channel_config.get("user_name"))
                sender_password = decrypt(channel_config.get("password"))
                outlook_relay_sender_email = channel_config.get("outlook_relay_sender_email", "")
                smtp_config = {
                    "host": channel_config.get("host"),
                    "port": channel_config.get("port"),
                    "ssl": channel_config.get("ssl", False),
                    "outlook_relay_sender_email": outlook_relay_sender_email,
                    "smtp_type": smtp_type,
                }
                success = send_via_smtp(
                    notification_info,
                    sender_email,
                    sender_password,
                    receivers_email,
                    smtp_config,
                    is_summary_notification
                )

        if smtp_type == GMAIL:
            sender_email = decrypt(channel_config.get("user_name"))
            sender_password = decrypt(channel_config.get("password"))
            smtp_config = {
                "host": channel_config.get("host"),
                "port": channel_config.get("port"),
                "ssl": channel_config.get("ssl", False),
            }
            success = send_via_smtp(
                notification_info,
                sender_email,
                sender_password,
                receivers_email,
                smtp_config,
                is_summary_notification
            )
        if smtp_type == PROOFPOINT:
            if proofpoint_integration_type == "rest_api":
                success = send_via_proofpoint_rest_api(notification_info, receivers_email, is_summary_notification)
            elif proofpoint_integration_type == "proofpoint_smtp":
                success = send_via_proofpoint_smtp(notification_info, receivers_email, is_summary_notification)
        if smtp_type == AWS_SES:
            if aws_ses_auth_type == "username_password":
                success = send_via_aws_ses_username_password(notification_info, receivers_email, is_summary_notification)
            else:
                success = send_via_aws_ses(notification_info, receivers_email, is_summary_notification)

        if success and not is_report and not is_workflow:
            update_notification_status(notification_info, SUCCESS)
    except Exception as e:
        log_error("send_mail : error to send email", e)
        if not is_report and not is_workflow:
            update_notification_status(notification_info, FAILED, e)
        raise e


def send_via_sendgrid(notification_info: dict, api_key: str, receivers_email: list, is_summary_notification: bool):
    try:
        if not receivers_email:
            return
        sendgrid_client = SendGridAPIClient(api_key)
        message_content = prepare_message(notification_info, receivers_email, is_summary_notification)
        response = sendgrid_client.send(message_content)
        return True
    except Exception as e:
        log_error("send_mail : error in SendGrid", e)
        raise e


def _get_ms_graph_token(client_id: str, tenant_id: str, client_secret: str) -> str:
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    payload = {
        "grant_type": "client_credentials",
        "scope": "https://graph.microsoft.com/.default",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    try:
        token_response = requests.post(token_url, data=payload)
        token_response.raise_for_status()
        return token_response.json().get("access_token")
    except Exception as e:
        log_error("Failed to obtain MS Graph token", e)
        return None


def _convert_to_graph_format(mime_message, receivers_email: list) -> dict:
    subject = mime_message["Subject"]

    if mime_message.is_multipart():
        for part in mime_message.walk():
            if part.get_content_type() == "text/html":
                body = part.get_payload()
                # Check if content is base64 encoded (happens when charset is specified)
                if isinstance(body, str) and not body.startswith('<') and len(body) > 50:
                    # Try to decode as base64
                    try:
                        decoded_body = part.get_payload(decode=True)
                        if isinstance(decoded_body, bytes):
                            body = decoded_body.decode('utf-8')
                    except Exception as e:
                        log_error(f"Failed to decode base64 content in multipart message: {str(e)}", e)
                break
    else:
        body = mime_message.get_payload()
        # Check if content is base64 encoded
        if isinstance(body, str) and not body.startswith('<') and len(body) > 50:
            try:
                decoded_body = mime_message.get_payload(decode=True)
                if isinstance(decoded_body, bytes):
                    body = decoded_body.decode('utf-8')
            except Exception as e:
                log_error(f"Failed to decode base64 content in single message: {str(e)}", e)

    to_recipients = [
        {"emailAddress": {"address": email.strip()}} for email in receivers_email
    ]

    attachments = []
    if mime_message.is_multipart():
        for part in mime_message.walk():
            if part.get_content_maintype() == "application":
                file_data = part.get_payload(decode=True)
                encoded_file = base64.b64encode(file_data).decode()

                attachments.append(
                    {
                        "@odata.type": "#microsoft.graph.fileAttachment",
                        "name": part.get_filename(),
                        "contentBytes": encoded_file,
                        "contentType": part.get_content_type(),
                    }
                )

    payload = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": body},
            "toRecipients": to_recipients,
        },
        "saveToSentItems": "true",
    }

    if attachments:
        payload["message"]["attachments"] = attachments

    return payload


def send_via_outlook(notification_info: dict, receivers_email: list, is_summary_notification: bool):
    try:
        if not receivers_email:
            return
        credentials = notification_info.get("channel_config", {})
        client_id = decrypt(credentials.get("client_id"))
        tenant_id = decrypt(credentials.get("tenant_id"))
        client_secret = decrypt(credentials.get("client_secret"))
        sender_email = credentials.get("sender_email")

        token = _get_ms_graph_token(client_id, tenant_id, client_secret)

        if not token:
            raise ValueError("Failed to obtain access token")

        message = prepare_message(notification_info, receivers_email, is_summary_notification)
        graph_payload = _convert_to_graph_format(message, receivers_email)
        email_url = f"https://graph.microsoft.com/v1.0/users/{sender_email}/sendmail"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }
        response = requests.request(
            "POST", email_url, headers=headers, data=json.dumps(graph_payload)
        )
        response.raise_for_status()
        return True
    except Exception as e:
        log_error("send_mail : error in Outlook", e)
        raise e


def send_via_smtp(
    notification_info: dict,
    sender_email: str,
    sender_password: str,
    receivers_email: list,
    smtp_config: dict,
    is_summary_notification: bool
):
    mailserver = None
    try:
        if not receivers_email:
            return
        use_ssl = smtp_config.get("ssl", False)
        port = int(smtp_config.get("port", 587))
        host = smtp_config.get("host")

        if port == 25:
            mailserver = smtplib.SMTP(host)
            mailserver.ehlo()
            if smtp_config.get("smtp_type") == OUTLOOK:
                mailserver.starttls()
                mailserver.login(sender_email, sender_password)
        elif use_ssl == True or use_ssl == "true":
            mailserver = smtplib.SMTP_SSL(host, port)
            mailserver.ehlo()
            mailserver.starttls()
            mailserver.login(sender_email, sender_password)
        else:
            mailserver = smtplib.SMTP(host, port)
            mailserver.ehlo()
            mailserver.starttls()
            mailserver.login(sender_email, sender_password)

        if smtp_config.get("smtp_type") == OUTLOOK:
            if smtp_config.get("outlook_relay_sender_email", ""):
                sender_email = smtp_config.get("outlook_relay_sender_email")
                channel_config = notification_info.get("channel_config", None)
                channel_config.update({"sender_email" : sender_email, "is_outlook_relay" : True})
                notification_info.update({"channel_config" :channel_config})

        message_content = prepare_message(notification_info, receivers_email, is_summary_notification)
        mailserver.sendmail(sender_email, receivers_email, message_content.as_string())
        return True
    except Exception as e:
        log_error("send_mail : error in SMTP", e)
        if mailserver:
            mailserver.close()
        raise e


def send_via_aws_ses(notification_info: dict, receivers_email: list, is_summary_notification: bool):
    try:
        if not receivers_email:
            return
        chanel_config = notification_info.get("channel_config", None)
        aws_access_key = decrypt(chanel_config.get("aws_access_key"))
        aws_secret_access_key = decrypt(chanel_config.get("aws_secret_access_key"))
        region = chanel_config.get("region")

        ses_client = boto3.client(
            "ses",
            region_name=region,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_access_key,
        )

        message_content = prepare_message(notification_info, receivers_email, is_summary_notification)
        ses_client.send_raw_email(
            Source=message_content["From"],
            Destinations=receivers_email,
            RawMessage={
                "Data": message_content.as_string(),
            },
        )
        return True
    except Exception as e:
        log_error("send_mail : error in AWS SES", e)
        raise e


def send_via_aws_ses_username_password(notification_info: dict, receivers_email: list, is_summary_notification: bool):
    server = None
    try:
        if not receivers_email:
            return
        channel_config = notification_info.get("channel_config", None)
        smtp_host = channel_config.get("host")
        smtp_port = channel_config.get("port")
        username = decrypt(channel_config.get("user_name"))
        password = decrypt(channel_config.get("password"))
        sender_email = decrypt(channel_config.get("sender_email"))        
        if not smtp_host or not smtp_port or not username or not password or not sender_email:
            print("Missing required credentials: host, port, user_name, password, or sender_email")        
        smtp_port = int(smtp_port) if smtp_port else 587
        
        # Connect to SMTP server
        if smtp_port == 465:
            server = smtplib.SMTP_SSL(smtp_host, smtp_port)
        elif smtp_port == 25:
            server = smtplib.SMTP(smtp_host, smtp_port)
            server.ehlo()
            server.starttls()
        else:  # 587 or other ports
            server = smtplib.SMTP(smtp_host, smtp_port)
            server.ehlo()
            server.starttls()
        
        server.login(username, password)

        for receiver in receivers_email:
            receiver = [receiver]
            try:
                message_content = prepare_message(notification_info, receiver, is_summary_notification)
                server.sendmail(sender_email, receiver, message_content.as_string())
            except Exception as e:
                log_error("send_mail : error in AWS SES username password", e)
                continue
        server.quit()
        return True
    except Exception as e:
        log_error("send_mail : error in AWS SES username password", e)
        if server:
            server.quit()
        raise e

def prepare_message(notification_info: dict, receivers_email, is_summary_notification: bool ):
    try:
        channel_config = notification_info.get("channel_config", None)
        module_name = notification_info.get("module_name", "")
        notitication_type = notification_info.get("notification_type", "")
        if notitication_type == "issue_update":
            module_name = notitication_type
        smtp_type = channel_config.get("smtp_type", GMAIL)
        is_outlook_relay = channel_config.get("is_outlook_relay", False)
        sender_email = (
            decrypt(channel_config.get("user_name"))
            if (smtp_type not in {SEND_GRID, AWS_SES, PROOFPOINT} or (smtp_type == OUTLOOK and not is_outlook_relay))
            else channel_config.get("sender_email")
        )
        sender_name = notification_info.get("sender_name", "")
        attachments = notification_info.get("attachments", [])
        mail_subject = notification_info.get("subject")
        mail_body = notification_info.get("body")
        attached_files = notification_info.get("attached_files", [])

        mail_content = notification_info.get("content", {})
        mail_params = mail_content.get("params", {})
        summary_notification_data = mail_content.get("summary_notification_data", [])
        default_icons = get_dqlabs_default_images()
        client_details = get_client_details(notification_info)
        subject = mail_params.get("subject")
        if subject:
            mail_subject = subject
        if client_details:
            default_icons.update(client_details)

        dag_info = notification_info.get("dag_info")
        client_domain = get_client_domain(dag_info)

        image = mail_content.get("image", None)
        owner_name = mail_content.get("owner_name", None)
        subject_description = mail_content.get("subject_description", "")

        for key in mail_params:
            value = mail_params[key] if mail_params[key] else ""
            mail_body = mail_body.replace(f"[{key}]", value)
            mail_subject = mail_subject.replace(f"[{key}]", value)

        if is_summary_notification and summary_notification_data:
            summary_template = get_summary_template(module_name, "email")
            content = ""
            for params in summary_notification_data:
                template_content = summary_template
                for key in params:
                    value = params[key] if params[key] else ""
                    template_content = template_content.replace(f"[{key}]", value)
                content += template_content
            mail_body = mail_body.replace("[summary_template]", content)
                
            
        for key in default_icons:
            value = default_icons[key] if default_icons[key] else ""
            mail_body = mail_body.replace(f"[{key}]", value)
            mail_subject = mail_subject.replace(f"[{key}]", value)

        if owner_name:
            mail_subject = mail_subject.replace("[sender_name]", owner_name).replace(
                "[subject_description]", subject_description
            )
            mail_body = mail_body.replace("[sender_name]", owner_name)
        else:
            mail_subject = mail_subject.replace("[sender_name]", sender_name).replace(
                "[subject_description]", subject_description
            )
            mail_body = mail_body.replace("[sender_name]", sender_name)

        if image:
            mail_body = mail_body.replace(f"[attachment]", image)

        if attachments and len(attachments) > 0:
            mail_body = mail_body.replace(
                f"[attachment]", f"""{attachments[0].get("attachment", '')}"""
            )

        if client_domain:
            mail_body = mail_body.replace(f"[domain]", client_domain)


        if 'Enter Your Text Here' in mail_body:
            mail_body = mail_body.replace(f"Enter Your Text Here", "")

        if smtp_type != SEND_GRID:
            message = MIMEMultipart("mixed")
            # Turn these into plain/html MIMEText objects
            mimeHtmlContent = MIMEText(mail_body, "html")

            # Add HTML content first
            message.attach(mimeHtmlContent)

            # Add file attachments
            for path in attached_files:
                part = MIMEBase("application", "octet-stream")
                with open(path, "rb") as file:
                    part.set_payload(file.read())
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    "attachment; filename={}".format(Path(path).name),
                )
                message.attach(part)

            message["Subject"] = mail_subject
            message["From"] = sender_email
            receivers_email = ["v-ramangouda_patil@carmax.com"] if smtp_type == PROOFPOINT else receivers_email
            message["To"] = ", ".join(receivers_email)
        else:
            message = Mail(
                from_email=sender_email,
                to_emails=receivers_email,
                subject=mail_subject,
                html_content=mail_body,
            )
            for path in attached_files:
                with open(path, "rb") as file:
                    data = file.read()
                    file.close()
                encoded = base64.b64encode(data).decode()
                attachment = Attachment()
                attachment.file_content = FileContent(encoded)
                attachment.file_type = FileType(Path(path).name)
                attachment.file_name = FileName(path)
                attachment.disposition = Disposition("attachment")
                attachment.content_id = ContentId(path)
                message.attachment = attachment
        return message
    except Exception as e:
        log_error("send_mail : prepare_message", e)
        raise e

def send_via_proofpoint_rest_api(notification_info: dict, receivers_email: list, is_summary_notification: bool):
    try:
        if not receivers_email:
            return
        channel_config = notification_info.get("channel_config", None)
        client_id = decrypt(channel_config.get("client_id"))
        client_secret = decrypt(channel_config.get("client_secret"))
        region = channel_config.get("region")
        sender_email = channel_config.get("sender_email")
        
        # Get access token
        token_url = f"https://mail-{region.lower()}.ser.proofpoint.com/v1/token"
        token_payload = {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials"
        }
        token_headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json"
        }
        
        token_response = requests.post(token_url, data=token_payload, headers=token_headers)
        token_response.raise_for_status()
        access_token = token_response.json().get("access_token")
        
        if not access_token:
            raise ValueError("Failed to obtain access token from Proofpoint")
        # Prepare message content
        message_content = prepare_message(notification_info, receivers_email, is_summary_notification)
        
        # Extract subject and body from the prepared message
        mail_subject = message_content["Subject"]
        mail_body = None
        
        if message_content.is_multipart():
            for part in message_content.walk():
                if part.get_content_type() == "text/html":
                    mail_body = part.get_payload(decode=True).decode('utf-8')
                    break
        else:
            mail_body = message_content.get_payload(decode=True).decode('utf-8')
        
        if not mail_body:
            raise ValueError("Failed to extract email body from prepared message")
        
        # Prepare recipients
        tos = [{"email": email.strip()} for email in receivers_email]
        
        # Send email via Proofpoint REST API
        send_url = f"https://mail-{region.lower()}.ser.proofpoint.com/v1/send"
        send_payload = {
            "content": [
                {
                    "body": mail_body,
                    "type": "text/html"
                }
            ],
            "from": {"email": sender_email},
            "headers": {"from": {"email": sender_email}},
            "subject": mail_subject,
            "tos": tos
        }
        send_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}"
        }
        send_response = requests.post(send_url, json=send_payload, headers=send_headers)
        send_response.raise_for_status()
        return True
    except Exception as e:
        log_error("send_mail : error in Proofpoint REST API", e)
        raise e

def send_via_proofpoint_smtp(notification_info: dict, receivers_email: list, is_summary_notification: bool):
    server = None
    try:
        if not receivers_email:
            return
        channel_config = notification_info.get("channel_config", None)
        smtp_server = channel_config.get("smtp_server") or channel_config.get("host")
        smtp_port = channel_config.get("port")
        username = decrypt(channel_config.get("username") or channel_config.get("user_name"))
        password = decrypt(channel_config.get("password"))
        sender_email = channel_config.get("sender_email")
        if not smtp_server or not smtp_port or not username or not password or not sender_email:
            raise ValueError("Missing required credentials: smtp_server/host, port, username/user_name, password, or sender_email")
        
        smtp_port = int(smtp_port) if smtp_port else 587
        
        # Prepare message content
        message_content = prepare_message(notification_info, receivers_email, is_summary_notification)
        # Connect to SMTP server based on port
        if smtp_port == 465:
            # Port 465 uses SSL
            context = ssl.create_default_context()
            server = smtplib.SMTP_SSL(smtp_server, smtp_port, context=context)
        else:
            # Port 587 or other ports use STARTTLS
            context = ssl._create_unverified_context()
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls(context=context)
        
        server.login(username, password)
        server.sendmail(sender_email, receivers_email, message_content.as_string())
        return True
    except Exception as e:
        log_error("send_mail : error in Proofpoint SMTP", e)
        if server:
            server.quit()
        raise e
