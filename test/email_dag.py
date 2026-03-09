import airflow
from pathlib import Path
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from dqlabs.utils import load_env
from dqlabs.app_helper.dag_helper import default_args
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


# load the env variables
base_path = str(Path(__file__).parents[0])
load_env(base_path)

def send_mail():
    mailserver = smtplib.SMTP("smtp.gmail.com", 587)
    mailserver.set_debuglevel(1)
    mailserver.ehlo()
    mailserver.starttls()
    mailserver.login("support@dqlabsai.net", "yzbhykkxirwwdftk")


    receivers_email = ['kirubakaran@intellectyx.com']
    mail_body = "<html><body><div><b>Welcome from sample mail</b></div></body></html>"

    mail_body = """
        <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
        <html lang="en" xml:lang="en" xmlns="http://www.w3.org/1999/xhtml">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=Windows-1252">
                <title>Welcome to DQLabs</title>
                <link href="https://fonts.googleapis.com/css2?family=Roboto:wght@400;500;700;900&display=swap" rel="stylesheet">
            </head>
            <body style="background: #EFEFEF;">
                <table border="0" cellpadding="0" cellspacing="0" align="center"
                    style="height: 100%;border: 1px solid #dfdfdf;padding:40px">
                    <tbody>
                        <tr>
                            <td style="padding: 30px 30px 15px 30px;background-color: #fff;">
                                <table border="0" cellpadding="0" cellspacing="0" align="center" style="background-color: #fff;">
                                    <tbody>
                                        <tr>
                                            <td align="center" style="padding-bottom:5px;"><img style="max-width:100px"
                                                    src='[logo]' /> </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </td>
                        </tr>
                        <tr>
                            <td style="padding: 20px 30px 0px 30px; background: #fff;">
                                <table border="0" cellpadding="0" cellspacing="0" align="center"
                                    style="padding: 0px 30px 0px 30px;background-color: #E60000; width: 100%;">
                                    <tbody>
                                        <tr>
                                            <td align="left" style="height: 150px;">
                                                <p class="text"
                                                    style="margin: 0px; font-family: 'Roboto', sans-serif !important; color: #fff; font-size: 38px; font-weight: 500; text-align: left; margin-bottom: 0px; line-height: 45px;">
                                                    Welcome to DQLabs</p>
                                            </td>
                                            <td style="text-align: right;"> <img style="height: 150px;" src='[income_mail_icon]' />
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </td>
                        </tr>
                        <tr>
                            <td style="padding: 30px 50px 30px 50px;background-color: #fff; width: 100%;">
                                <table border="0" cellpadding="0" cellspacing="0" align="center"
                                    style="background-color: #fff; width: 100%;">
                                    <tbody>
                                        <tr>
                                            <td align="left" style="padding-bottom:15px;padding-top:10px;">
                                                <p class="text"
                                                    style="margin: 0px; font-family: 'Roboto', sans-serif !important; color: #1d1d1d; font-size: 24px; font-weight: 500; text-align: center; margin-bottom: 10px; text-align: left; line-height: 21px;">
                                                    Hello,</p>
                                            </td>
                                        </tr>
                                        <tr>
                                            <td align="left" style="padding-bottom:20px;">
                                                <p class="text"
                                                    style="margin: 0px; font-family: 'Roboto', sans-serif !important; color: #4A4A4A; font-size: 16px; font-weight: 400; line-height: 21px;">
                                                    You have a new Account with the DQLabs platform.
                                                </p>
                                            </td>
                                        </tr>
                                        <tr>
                                            <td align="left" style="padding-bottom:30px;">
                                                <p class="text"
                                                    style="margin: 0px; font-family: 'Roboto', sans-serif !important; color: #4A4A4A; font-size: 16px; font-weight: 400; line-height: 21px;">
                                                    Sign in to your DQLabs Account to access the services your organization provides
                                                    and collaborate with your team.
                                                </p>
                                            </td>
                                        </tr>
                                        <tr>
                                            <td align="left" style="padding-bottom:20px;">
                                                <p class="text"
                                                    style="margin: 0px; font-family: 'Roboto', sans-serif !important; color: #4A4A4A; font-size: 16px; font-weight: 400; line-height: 21px;">
                                                    <b>Your Username:</b> [user_name]
                                                </p>
                                            </td>
                                        </tr>
                                        <tr>
                                            <td align="left" style="padding-bottom:20px;">
                                                <p class="text"
                                                    style="margin: 0px; font-family: 'Roboto', sans-serif !important; color: #4A4A4A; font-size: 16px; font-weight: 400; line-height: 21px;">
                                                    <b>Password:</b> Click Sign in below to set your password and sign in. To keep your account secure, follow the password guidelines.
                                                </p>
                                            </td>
                                        </tr>
                                        <tr>
                                            <td style="padding-bottom:10px; padding-top: 20px;">
                                                <table align="center" border="0" cellpadding="0" cellspacing="0"
                                                    style="text-align: center;">
                                                    <tbody>
                                                        <tr>
                                                            <td align="center">
                                                                <table align="center" style="max-width: 130px;display: inline-block;border-style: none;background-color: #E60000;padding: 8px 20px;color: #fff;">
                                                                    <tbody>
                                                                        <tr>
                                                                            <td align="center">
                                                                                <a target="_blank" href="https://conti.dqlabsai.net/" class="btn saveseat"
                                                                                    style="font-size: 16px;font-family: 'Roboto', sans-serif !important; color: #FFFFFF; font-weight: 500; text-decoration: none; white-space: nowrap; display: inline-block; text-transform: uppercase; background: #E60000; text-decoration: none;">Sign
                                                                                    In</a>
                                                                            </td>
                                                                        </tr>
                                                                    </tbody>
                                                                </table>
                                                            </td>
                                                        </tr>
                                                        <tr>
                                                            <td align="left" style="padding-top:40px;">
                                                                <p class="text"
                                                                    style="margin: 0px; font-family: 'Roboto', sans-serif !important; color: #4A4A4A; font-size: 16px; font-weight: 400; line-height: 21px;">
                                                                    For your security, the reset password link expires after 48
                                                                    hours. After that, please contact your <a
                                                                        href="mailto:support@dqlabsai.net">administrator</a> for your
                                                                    password.
                                                                </p>
                                                            </td>
                                                        </tr>
                                                        <tr>
                                                            <td align="center" style="padding-top: 20px;">
                                                                <p class="btn saveseat"
                                                                    style="font-family: 'Roboto', sans-serif !important; font-size: 12px; color: #c2c2c2; line-height: 18px; text-decoration: none; text-align: center; display: inline-block; text-decoration: none;">
                                                                    You received this email because you signed up for DQLABS account
                                                                    with this emailaddress. <br />Please ignore this email, if this
                                                                    was a
                                                                    mistake the account hasn't been created yet.</p>
                                                            </td>
                                                        </tr>
                                                    </tbody>
                                                </table>
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </td>
                        </tr>
                        <tr>
                            <td align="center" style="padding: 30px;background-color: #f8f8f8;">
                                <table border="0" cellpadding="0" cellspacing="0" align="center"
                                    style="background-color: #f8f8f8;height: 100%;width: 100%;">
                                    <tbody>
                                        <tr>
                                            <td align="left" style="padding-bottom:6px;" border="0">
                                                <table border="0" cellpadding="0" cellspacing="0" align="left">
                                                    <tbody>
                                                        <tr>
                                                            <td border="0">
                                                                <p style="display: flex; align-items: center;"> <img src='[mail_icon]' /> <a href="mailto:support@dqlabsai.net"
                                                                        class="link"
                                                                        style="color: #979797; text-decoration: none; font-family: 'Roboto', sans-serif !important; font-size: 13.5px; font-weight: 400; padding-left: 5px;">
                                                                        support@dqlabsai.net</a> </p>
                                                            </td>
                                                        </tr>
                                                    </tbody>
                                                </table>
                                            </td>
                                            <td align="right" style="padding-bottom:6px;" border="0">
                                                <table border="0" cellpadding="0" cellspacing="0" align="right">
                                                    <tbody>
                                                        <tr>
                                                            <td border="0">
                                                                <p style="display: flex; align-items: center;"> <img
                                                                        src='[website_icon]' /> <a target="_blank"
                                                                        href="https://www.dqlabs.ai/" class="link"
                                                                        style="color: #979797; text-decoration: none; font-family: 'Roboto', sans-serif !important; font-size: 13.5px; font-weight: 400; padding-left: 5px;">
                                                                        dqlabs.ai</a> </p>
                                                            </td>
                                                        </tr>
                                                    </tbody>
                                                </table>
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </td>
                        </tr>
                    </tbody>
                </table>
            </body>
        </html>
    """

    message = MIMEMultipart("multipart")
    # Turn these into plain/html MIMEText objects
    mimeHtmlContent = MIMEText(mail_body, "html")
    message.attach(mimeHtmlContent)
    message["Subject"] = "Sample Email"
    message["From"] = "support@dqlabsai.net"
    # message["To"] =  ", ".join(receivers_email)

    mailserver.sendmail(
        "support@dqlabsai.net", receivers_email, message.as_string()
    )
    

dag = DAG(
    dag_id="email_dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,
    is_paused_upon_creation=False,
    concurrency=1,
    max_active_tasks=1,
)
with dag:
    connection_task = PythonOperator(
        task_id="send_mail",
        python_callable=send_mail
    )

globals()[dag.dag_id] = dag
