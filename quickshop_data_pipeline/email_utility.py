"""Email utility module for sending validation confirmation emails."""
import smtplib as sm
from email.mime.text import MIMEText as mt

import datetime as dt

import file_operation as fo

def setup_mail(total_files, error_files):
    """Create email message with validation summary.

    Args:
        total_files: Total number of files processed.
        error_files: Number of files that failed validation.

    Returns:
        MIMEText message object.
    """
    msg_body = f'Total {total_files} incoming files processed.\n' \
               f'{total_files - error_files} files passed validation.'

    date_subject = dt.date.today().strftime("%Y-%m-%d")

    msg = mt(msg_body)
    msg['Subject'] = f'validation email {date_subject}'
    msg['From'] = fo.msg_sender
    msg['To'] = fo.msg_reciever

    return msg


def send_mail(msg: mt):
    """Send email message via SMTP.

    Args:
        msg: MIMEText message object to send.
    """
    with sm.SMTP(fo.msg_server, fo.msg_port) as server:
        server.starttls()  # Secure the connection
        server.login(fo.msg_sender, fo.msg_password)  # Use app password if Gmail
        server.send_message(msg)
