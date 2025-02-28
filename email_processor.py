import imaplib
import email
import uuid
import psycopg2
from datetime import datetime
import re
from email.utils import parseaddr
from configparser import ConfigParser
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import gc
import time
import os
import psycopg2.extras

from template import EmailTemplate, TemplateArguments
logger = logging.getLogger(__name__)

class EmailProcessor:
    def __init__(self):
        self.config = self._load_config()
        self.conn = None
        self.mail = None
        self.smtp = None

    def _load_config(self):
        config = ConfigParser()
        env = os.getenv('ENV', 'development')
        if os.path.exists(f'config.{env}.ini'):
            config.read(f'config.{env}.ini')
        elif os.path.exists('config.ini'):
            config.read('config.ini')
        else:
            self.config = {
                'company': {
                    'name': os.getenv('COMPANY_NAME', 'Helpihub'),
                    'domain': os.getenv('COMPANY_DOMAIN', 'http://localhost:3000')
                },
                'database': {
                    'host': os.getenv('DATABASE_HOST', 'localhost'),
                    'port': os.getenv('DATABASE_PORT', 5432),
                    'user': os.getenv('DATABASE_USER', 'postgres'),
                    'password': os.getenv('DATABASE_PASS', 'postgres'),
                    'dbname': os.getenv('DATABASE_NAME', 'itsm')
                },
                'email': {
                    'imap_host': os.getenv('EMAIL_IMAP_HOST', None),
                    'imap_port': os.getenv('EMAIL_IMAP_PORT', None),
                    'smtp_host': os.getenv('EMAIL_SMTP_HOST', None),
                    'smtp_port': os.getenv('EMAIL_SMTP_PORT', None),
                    'username': os.getenv('EMAIL_USERNAME', None),
                    'password': os.getenv('EMAIL_PASSWORD', None),
                    'sender_name': os.getenv('EMAIL_SENDER_NAME', 'Helpihub Support'),
                    'sender_address': os.getenv('EMAIL_SENDER_ADDRESS', None)
                }
            }
        return config

    def _connect_db(self):
        if not self.conn or self.conn.closed:
            self.conn = psycopg2.connect(
                dbname=self.config['database']['dbname'],
                user=self.config['database']['user'],
                password=self.config['database']['password'],
                host=self.config['database']['host']
            )
        return self.conn

    def _connect_imap(self):
        """IMAP connection with error handling"""
        mail = None
        try:
            if self.mail:
                try:
                    self.mail.close()
                    self.mail.logout()
                except:
                    logger.warning("Failed to properly close existing IMAP connection")
                    pass
                self.mail = None
                gc.collect()
            
            logger.debug("Establishing new IMAP connection")
            mail = imaplib.IMAP4_SSL(
                host=self.config['email']['imap_host'],
                timeout=30
            )
            mail.login(self.config['email']['username'], self.config['email']['password'])
            logger.debug("IMAP connection established successfully")
            return mail
        except Exception as e:
            logger.error(f"IMAP connection error: {str(e)}")
            if mail:
                try:
                    mail.logout()
                except:
                    logger.warning("Failed to logout from IMAP after connection error")
                    pass
                mail = None
            gc.collect()
            raise

    def _connect_smtp(self):
        """Establish SMTP connection"""
        if not self.smtp:
            self.smtp = smtplib.SMTP_SSL(self.config['email']['smtp_host'])
            self.smtp.login(self.config['email']['username'], self.config['email']['password'])
        return self.smtp

    def process_emails(self):
        """Process emails with improved resource management"""
        mail = None
        try:
            logger.debug("Starting email processing cycle")
            self.conn = self._connect_db()
            
            max_retries = 3
            retry_delay = 5  # seconds
            
            for attempt in range(max_retries):
                try:
                    mail = self._connect_imap()
                    break
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    logger.warning(f"Connection attempt {attempt + 1} failed, waiting {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    gc.collect()
            
            mail.select('inbox')
            _, messages = mail.search(None, 'UNSEEN')
            
            if not messages[0]:
                logger.debug("No new messages found")
                return
            
            logger.info(f"Found {len(messages[0].split())} new messages")
            
            # Process each email
            for message_number in messages[0].split():
                try:
                    _, msg_data = mail.fetch(message_number, '(RFC822)')
                    email_body = msg_data[0][1]
                    email_message = email.message_from_bytes(email_body)
                    
                    try:
                        self._process_single_email(email_message)
                    finally:
                        # Free memory immediately
                        del email_message
                        del email_body
                        del msg_data
                        gc.collect()
                
                except Exception as e:
                    logger.error(f"Fehler bei der Verarbeitung einer E-Mail: {str(e)}")
                    continue
        finally:
            # Clean up all connections
            if mail:
                try:
                    mail.close()
                    mail.logout()
                except:
                    pass
            gc.collect()

    def _cleanup(self):
        """Clean up all connections"""
        # Clean up IMAP connection
        if self.mail:
            try:
                self.mail.close()
                self.mail.logout()
            except:
                pass
            self.mail = None

        # Clean up SMTP connection
        if self.smtp:
            try:
                self.smtp.quit()
            except:
                pass
            self.smtp = None

        # Clean up database connection
        if self.conn and not self.conn.closed:
            try:
                self.conn.close()
            except:
                pass
            self.conn = None
        
        gc.collect()

    def _process_single_email(self, email_message):
        message_id = email_message.get('message-id', '').strip('<>')
        
        with self.conn.cursor() as cur:
            try:
                cur.execute("BEGIN")
                
                # Prüfe ob diese E-Mail bereits verarbeitet wurde
                cur.execute("""
                    SELECT id FROM items 
                    WHERE message_id = %s AND type = 'email'
                    FOR UPDATE SKIP LOCKED
                """, (message_id,))
                
                if cur.fetchone():
                    logger.warning(f"Email with message-id {message_id} already processed")
                    return
                
                # Email Details extrahieren
                subject = email_message.get('subject', '')
                from_addr = parseaddr(email_message.get('from'))[1]
                to_addr = parseaddr(email_message.get('to'))[1]
                in_reply_to = email_message.get('in-reply-to', '').strip('<>')
                references = email_message.get('references', '').split()
                if references:
                    references = [ref.strip('<>') for ref in references]

                # Zuerst Ticket erstellen oder finden
                ticket_id = None
                if in_reply_to or references:
                    # Suche nach verknüpftem Ticket
                    message_refs = [in_reply_to] + (references if references else [])
                    message_refs = [ref for ref in message_refs if ref]
                    
                    if message_refs:
                        cur.execute("""
                            SELECT ticket_id
                            FROM items
                            WHERE message_id = ANY(%s)
                            AND type = 'email'
                            LIMIT 1
                        """, (message_refs,))
                        result = cur.fetchone()
                        if result:
                            ticket_id = result[0]

                # Wenn kein existierendes Ticket gefunden, erstelle ein neues
                if not ticket_id:
                    ticket_id, ticket_number = self._create_new_ticket(cur, subject, from_addr)
                
                # Speichere die ursprüngliche E-Mail
                email_body = self._get_email_body(email_message)
                item_id = self._store_email_in_db(
                    email_message,  # Original-E-Mail
                    ticket_id,
                    email_body,
                    in_reply_to,
                    references
                )
                
                # Sende Bestätigungsmail nur für neue Tickets
                if not in_reply_to and not references:
                    self._send_confirmation_email(
                        to_address=from_addr,
                        ticket_number=ticket_number,
                        subject=subject,
                        ticket_id=ticket_id,
                        body=email_body,
                        original_message_id=message_id  # Übergebe die Original Message-ID
                    )

                cur.execute("COMMIT")
                logger.info(f"Successfully processed email {message_id} for ticket {ticket_id}")
                return ticket_id

            except Exception as e:
                cur.execute("ROLLBACK")
                logger.error(f"Error processing email: {str(e)}")
                raise

    def _get_email_body(self, email_message):
        """Get email body"""
        # Initialize body   
        body = ""
        # Check if email is multipart
        if email_message.is_multipart():
            # Walk through parts of the email
            for part in email_message.walk():
                if part.get_content_type() == "text/plain":
                    body = part.get_payload(decode=True).decode()
                    break
        else:
            body = email_message.get_payload(decode=True).decode()
        return body

    def _extract_ticket_reference(self, subject):
        """Extract ticket reference from subject"""
        pattern = r'#([A-Z]+-\d+)'
        match = re.search(pattern, subject)
        return match.group(1) if match else None

    def _create_new_ticket(self, cur, subject, from_addr):
        ticket_id = str(uuid.uuid4())
        
        # Hole die nächste Ticket-Nummer
        cur.execute("SELECT nextval('ticket_number_seq')")
        ticket_number = cur.fetchone()[0]
        
        # Hole die Default Queue
        cur.execute("""
            SELECT id FROM queues 
            WHERE id = '11111111-1111-1111-1111-111111111111'
            OR name = 'Default Queue'
            LIMIT 1
        """)
        result = cur.fetchone()
        if not result:
            # Erstelle Default Queue falls nicht vorhanden
            queue_id = '11111111-1111-1111-1111-111111111111'
            cur.execute("""
                INSERT INTO queues (id, name, prefix, description)
                VALUES (%s, 'Default Queue', 'SUP', 'Default Support Queue')
                ON CONFLICT (id) DO NOTHING
                RETURNING id
            """, (queue_id,))
        else:
            queue_id = result[0]
        
        # Erstelle neues Ticket
        now = datetime.now()
        cur.execute("""
            INSERT INTO tickets (
                id, ticket_number, subject, 
                queue_id, status_name, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, 'New', %s, %s)
        """, (
            ticket_id,
            f"SUP-{ticket_number:06d}",
            subject,
            queue_id,
            now,  # created_at
            now   # updated_at
        ))
        
        return ticket_id, f"SUP-{ticket_number:06d}"

    def _assign_supporter(self, cur, ticket_id):
        """Assign supporter to ticket"""
        # Get all active supporters
        cur.execute("""
            SELECT id, email 
            FROM supporters 
            ORDER BY 
                (SELECT COUNT(*) 
                 FROM tickets 
                 WHERE assigned_supporter_id = supporters.id 
                 AND status_name != 'Closed')
            LIMIT 1
        """)
        
        supporter = cur.fetchone()
        if supporter:
            supporter_id = supporter[0]
            
            cur.execute("""
                UPDATE tickets 
                SET assigned_supporter_id = %s 
                WHERE id = %s
            """, (str(supporter_id), ticket_id))
            
            cur.execute("""
                INSERT INTO ticket_assignments (id, ticket_id, supporter_id)
                VALUES (%s, %s, %s)
            """, (str(uuid.uuid4()), ticket_id, str(supporter_id)))

    def assign_supporter_to_ticket(self, ticket_id, supporter_id):
        """Assign supporter to ticket"""
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE tickets 
                SET assigned_supporter_id = %s 
                WHERE id = %s
            """, (str(supporter_id), str(ticket_id)))
            
            cur.execute("""
                INSERT INTO ticket_assignments (id, ticket_id, supporter_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (ticket_id, supporter_id) DO NOTHING
            """, (str(uuid.uuid4()), str(ticket_id), str(supporter_id)))
            
        self.conn.commit()

    def update_ticket_status(self, ticket_id: str, status_name: str):
        """Update the status of a ticket"""
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE tickets 
                SET status_name = %s
                WHERE id = %s
            """, (status_name, ticket_id))
            self.conn.commit()

    def _store_email_in_db(self, msg, ticket_id, body, original_email, references):
        """Speichert eine E-Mail in der Datenbank"""
        try:
            with self.conn.cursor() as cur:
                now = datetime.now()
                email_id = str(uuid.uuid4())
                
                # Wenn msg ein MIMEMultipart-Objekt ist, extrahiere die Header
                if isinstance(msg, (MIMEMultipart, MIMEText)):
                    message_id = msg['Message-ID'].strip('<>')
                    from_addr = msg['From']
                    to_addr = msg['To']
                    subject = msg['Subject']
                    in_reply_to = msg.get('In-Reply-To', '').strip('<>')
                    # Bestätigungsmails sind immer vom Supporter
                    source = 'supporter' if 'ticket-confirmation' in str(msg) else 'customer'
                else:
                    # Für normale E-Mail-Nachrichten
                    message_id = msg.get('Message-ID', '').strip('<>')
                    from_addr = msg.get('From', '')
                    to_addr = msg.get('To', '')
                    subject = msg.get('Subject', '')
                    in_reply_to = msg.get('In-Reply-To', '').strip('<>')
                    source = 'customer'  # Eingehende E-Mails sind immer vom Kunden
                
                # References als JSON vorbereiten
                refs_json = None
                if references:
                    clean_refs = [ref.strip('<>') for ref in references]
                    refs_json = psycopg2.extras.Json(clean_refs)
                
                # E-Mail speichern
                cur.execute("""
                    INSERT INTO items (
                        id, ticket_id, type, message_id, 
                        from_address, to_address, subject, 
                        body, received_at, in_reply_to, 
                        references_list, created_at, created_by_id,
                        source
                    )
                    VALUES (%s, %s, 'email', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    email_id,
                    ticket_id,
                    message_id,
                    from_addr,
                    to_addr,
                    subject,
                    body,
                    now,  # received_at
                    in_reply_to,
                    refs_json,
                    now,  # created_at
                    '22222222-2222-2222-2222-222222222222',  # created_by_id
                    source  # source ist jetzt korrekt für Bestätigungsmails
                ))
                
                result = cur.fetchone()
                if not result:
                    logger.error("Failed to store email in database - no ID returned")
                    raise Exception("Email storage failed")
                    
                logger.info(f"Successfully stored {source} email with ID {email_id} for ticket {ticket_id}")
                return email_id
                
        except Exception as e:
            logger.error(f"Error storing email in database: {str(e)}")
            raise

    def _store_comment_in_db(self, ticket_id: str, body: str):
        """Speichert einen Kommentar in der Datenbank"""
        try:
            with self.conn.cursor() as cur:
                now = datetime.now()
                comment_id = str(uuid.uuid4())
                
                cur.execute("""
                    INSERT INTO items (
                        id, ticket_id, type, body, 
                        received_at, created_at, created_by_id
                    )
                    VALUES (%s, %s, 'comment', %s, %s, %s, %s)
                    RETURNING id
                """, (
                    comment_id,
                    ticket_id,
                    body,
                    now,  # received_at
                    now,  # created_at
                    '22222222-2222-2222-2222-222222222222'  # created_by_id (System User)
                ))
                
                result = cur.fetchone()
                if not result:
                    logger.error("Failed to store comment in database - no ID returned")
                    raise Exception("Comment storage failed")
                    
                logger.info(f"Successfully stored comment with ID {comment_id} for ticket {ticket_id}")
                
        except Exception as e:
            logger.error(f"Error storing comment in database: {str(e)}")
            raise

    def _send_email(self, msg: MIMEMultipart):
        """Sendet eine E-Mail via SMTP"""
        smtp = None
        try:
            smtp = smtplib.SMTP_SSL(self.config['email']['smtp_host'])
            smtp.login(self.config['email']['username'], self.config['email']['password'])
            smtp.send_message(msg)
        finally:
            if smtp:
                smtp.quit()

    def _create_confirmation_email(self, to_address: str, ticket_number: str, 
                                 subject: str, body: str, ticket_id: str, original_message_id: str) -> tuple[MIMEMultipart, str]:
        """Erstellt die Bestätigungs-E-Mail und gibt (msg, body) zurück"""
        # Original-Email-Daten für Referenzen holen
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT message_id, in_reply_to, references_list
                FROM items 
                WHERE message_id = %s 
                AND type = 'email'
                ORDER BY created_at ASC
                LIMIT 1
            """, (original_message_id,))
            original_email = cur.fetchone()

        # E-Mail vorbereiten
        msg = MIMEMultipart()
        sender_name = self.config['email']['sender_name']
        sender_address = self.config['email']['sender_address']
        msg['From'] = f"{sender_name} <{sender_address}>"
        msg['To'] = to_address
        msg['Subject'] = f'[{ticket_number}] - {subject}'
        msg['Message-ID'] = f"<{uuid.uuid4()}@{self.config['email']['smtp_host']}>"
        
        # Set references and In-Reply-To headers
        references = []
        if original_email and original_email[0]:
            msg['In-Reply-To'] = f"<{original_email[0]}>"
            if original_email[2]:  # original references
                references.extend(original_email[2])
            if original_email[1]:  # original in-reply-to
                references.append(original_email[1])
            references.append(original_email[0])  # original message_id
            if references:
                msg['References'] = ' '.join(f"<{ref}>" for ref in references)

        company_image_url = None
        try:
            company_image_url = self.config['company']['image_url']
        except:
            pass
        company_name = self.config['company']['name']
        company_domain = self.config['company']['domain']
        company = None
        if company_image_url:
            company = f'<div class="company"><img src="{company_image_url}" alt="Company Logo" /></div>'
        else:
            company = f'<div class="company">{company_name}</div>'

        # Template rendern
        template = EmailTemplate(
            template_name="ticket-confirmation",
            arguments=[
                TemplateArguments(key="ticket_number", value=ticket_number),
                TemplateArguments(key="ticket_id", value=ticket_id),
                TemplateArguments(key="ticket_body", value=body),
                TemplateArguments(key="sender_name", value=sender_name),
                TemplateArguments(key="company", value=company),
                TemplateArguments(key="company_domain", value=company_domain)
            ]
        )
        body = template.render()
        msg.attach(MIMEText(body.strip(), 'html'))
        
        return msg, body, original_email, references

    def _send_confirmation_email(self, to_address: str, ticket_number: str, subject: str, ticket_id: str, body: str, original_message_id: str):
        """Hauptfunktion für das Senden der Bestätigungs-E-Mail"""
        try:
            # E-Mail erstellen
            msg, body, original_email, references = self._create_confirmation_email(
                to_address, ticket_number, subject, body, ticket_id, original_message_id
            )

            # Transaktion starten
            with self.conn.cursor() as cur:
                try:
                    cur.execute("BEGIN")
                    
                    # 1. E-Mail in DB speichern mit source='supporter'
                    cur.execute("""
                        INSERT INTO items (
                            id, ticket_id, type, message_id, 
                            from_address, to_address, subject, 
                            body, received_at, in_reply_to, 
                            references_list, created_at, created_by_id,
                            source
                        )
                        VALUES (%s, %s, 'email', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'supporter')
                        RETURNING id
                    """, (
                        str(uuid.uuid4()),  # id
                        ticket_id,
                        msg['Message-ID'].strip('<>'),
                        msg['From'],
                        msg['To'],
                        msg['Subject'],
                        body,
                        datetime.now(),  # received_at
                        msg.get('In-Reply-To', '').strip('<>'),
                        psycopg2.extras.Json(references) if references else None,
                        datetime.now(),  # created_at
                        '22222222-2222-2222-2222-222222222222'  # created_by_id
                    ))
                    
                    item_id = cur.fetchone()[0]
                    if not item_id:
                        raise Exception("Failed to store confirmation email")
                    
                    # 2. Thread-Beziehung speichern wenn es eine Antwort ist
                    if original_email and original_email[0]:
                        # Hole die ID der Original-Email
                        cur.execute("""
                            SELECT id 
                            FROM items 
                            WHERE message_id = %s 
                            AND type = 'email'
                            FOR UPDATE
                        """, (original_email[0],))
                        
                        parent_result = cur.fetchone()
                        if parent_result:
                            parent_id = parent_result[0]
                            # Speichere Thread-Beziehung
                            cur.execute("""
                                INSERT INTO item_threads (parent_item_id, child_item_id)
                                VALUES (%s, %s)
                            """, (parent_id, item_id))
                            logger.info(f"Created thread relationship: parent={parent_id}, child={item_id}")
                        else:
                            logger.warning(f"Could not find parent email with message_id {original_email[0]}")

                    # 3. E-Mail senden (innerhalb der Transaktion)
                    try:
                        self._send_email(msg)
                    except Exception as e:
                        logger.error(f"Failed to send email: {str(e)}")
                        raise

                    cur.execute("COMMIT")
                    logger.info(f"Successfully processed confirmation email for ticket {ticket_number}")
                    
                except Exception as e:
                    cur.execute("ROLLBACK")
                    logger.error(f"Transaction failed, rolling back: {str(e)}")
                    raise
            
        except Exception as e:
            logger.error(f"Failed to send/store confirmation email: {str(e)}")
            raise

    def __del__(self):
        self._cleanup()
  