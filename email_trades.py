#!/usr/bin/env python
# 
# # Purpose: Send email with stats report
import sys
import os
import smtplib 
from email.mime.multipart import MIMEMultipart 
from email.mime.text import MIMEText 
from email.mime.base import MIMEBase 
from email import encoders 

from config import EMAIL_FROM, EMAIL_PASSWD, EMAIL_RECIPIENTS

def send_email(to_addr: str, subject: str, body: str, filename: str = None) -> None:

    fromaddr = EMAIL_FROM

    # instance of MIMEMultipart 
    msg = MIMEMultipart() 

    # storing the senders email address 
    msg['From'] = fromaddr 

    # storing the receivers email address 
    msg['To'] = to_addr 

    # storing the subject 
    msg['Subject'] = subject

    # attach the body with the msg instance 
    msg.attach(MIMEText(body, 'plain')) 

    # open the file to be sent 
    if filename is not None:
        attachment = open(filename, "rb") 

        # instance of MIMEBase and named as p 
        p = MIMEBase('application', 'octet-stream') 

        # To change the payload into encoded form 
        p.set_payload((attachment).read()) 

        # encode into base64 
        encoders.encode_base64(p) 

        p.add_header('Content-Disposition', "attachment; filename= %s" % filename) 

        # attach the instance 'p' to instance 'msg' 
        msg.attach(p) 

    # creates SMTP session 
    s = smtplib.SMTP('smtp.gmail.com', 587) 

    # start TLS for security 
    s.starttls() 

    # Authentication 
    s.login(EMAIL_FROM, EMAIL_PASSWD)

    # Converts the Multipart msg into a string 
    text = msg.as_string() 

    # sending the mail 
    s.sendmail(EMAIL_FROM, to_addr, text) 

    # terminating the session 
    s.quit() 
    
if __name__ == "__main__":
    
    if len(sys.argv) != 3 :
        print(f'Syntax: {sys.argv[0]} <year> <week>')
        sys.exit(1)

    year = int(sys.argv[1])
    week = int(sys.argv[2])

    stats_dir = os.getenv('STATSDIR')
    if stats_dir is None:
        print("STATSDIR environment variable not set")
        sys.exit(1)
    filepath = os.path.join(stats_dir, str(year), f"Week{week:02}", f"week{week:02}.zip")
    
    for recipient in EMAIL_RECIPIENTS:
        send_email(recipient, f"Week{week:02} stats", f"The stats zip file should be attached. Week {week:02}, {year}.\n-Jon", filepath)
