import datetime as dt
import file_operation as fo
import smtplib as sm
from email.mime.text import MIMEText as mt

def setup_mail(total_files,error_files):
    # send confirmation mail
    msg_body = f'''Total {total_files} incoming files processed.\n{total_files-error_files} files passed validation.'''

    date_subject = dt.date.today().strftime("%Y-%m-%d")

    msg = mt(msg_body)
    msg['Subject'] = f'validation email {date_subject}'
    msg['From'] = fo.msg_sender
    msg['To'] = fo.msg_reciever

    return msg


def send_mail(msg: mt):
    with sm.SMTP(fo.msg_server,fo.msg_port) as server:
        server.starttls() # Secure the connection 
        server.login(fo.msg_sender, fo.msg_password) 
        # Use app password if Gmail 
        server.send_message(msg)
    
