# Standard
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import os
from typing import List
# External
import boto3

CLIENT_NAME = 'ses'

BODY_TYPES = ('plain', 'html')

def lambda_handler(event, context):
    print('EVENT', event)
    sender = event['sender']
    body = event['body']
    return SES(sender).send(event['to'], event['subject'],
        body_text = body.get('text'),
        body_html = body.get('html'),
        cc = event.get('cc'),
        bcc = event.get('bcc'),
        attachments = body.get('attachments'))

def parse_addresses(addresses: List[str] | str | None):
    if addresses is None:
        return []
    elif isinstance(addresses, str):
        return addresses.split(',')
    else:
        return addresses

class SES():
    
    def __init__(self,
            sender: str,
            client = None,
            profile: str | None = None):
        self.sender = sender
        if client is not None:
            self.client = client
        elif profile is not None:
            self.client = boto3.Session(profile_name = profile).client(CLIENT_NAME)
        else:
            self.client = boto3.client(CLIENT_NAME)

    def send(self,
            to: List[str] | str,
            subject: str,
            body_text: str | None = None,
            body_html: str | None = None,
            cc: List[str] | str | None = None,
            bcc: List[str] | str | None = None,
            attachments: List[str] | None = None,
            sender: str | None = None):
        if sender is None:
            sender = self.sender
        to = parse_addresses(to)
        cc = parse_addresses(cc)
        bcc = parse_addresses(bcc)
        destinations = to + cc + bcc
        # headers
        msg = MIMEMultipart('mixed')
        msg['Subject'] = subject 
        msg['From'] = sender
        msg['To'] = ', '.join(to)
        msg['Cc'] = ', '.join(cc)
        # body
        body = MIMEMultipart('alternative')
        for body_type, body_data in zip(BODY_TYPES, (body_text, body_html)):
            if body_data is not None:
                body.attach(MIMEText(body_data, body_type))
        msg.attach(body)
        # attachments
        if attachments is not None:
            for attachment in attachments:
                with open(attachment, 'rb') as f:
                    att = MIMEApplication(f.read())
                    att.add_header('Content-Disposition', 'attachment', filename = os.path.basename(attachment))
                    msg.attach(att)
        # build parameters and send
        kwargs = {
            'Source': sender,
            'Destinations': destinations,
            'RawMessage': { 'Data': msg.as_string() },
        }
        return self.client.send_raw_email(**kwargs)
