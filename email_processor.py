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

logger = logging.getLogger(__name__)

class EmailProcessor:
    def __init__(self):
        self.config = self._load_config()
        self.conn = None
        self.mail = None
        self.smtp = None

    def _load_config(self):
        config = ConfigParser()
        config.read('config.development.ini')
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
        subject = email_message.get('subject', '')
        from_addr = parseaddr(email_message.get('from'))[1]
        
        # Log email details
        logger.info(f"Received email - From: {from_addr}, Subject: {subject}")
        
        # Extract message-id, to, in-reply-to, and references
        message_id = email_message.get('message-id', '').strip('<>')
        to_addr = parseaddr(email_message.get('to'))[1]
        in_reply_to = email_message.get('in-reply-to', '').strip('<>')
        references = email_message.get('references', '').split()
        if references:
            references = [ref.strip('<>') for ref in references]

        logger.debug(f"Processing email details - Message-ID: {message_id}, In-Reply-To: {in_reply_to}")
        
        # Log email details
        logger.info(f"""Verarbeite E-Mail:
            Subject: {subject}
            Message-ID: {message_id}
            In-Reply-To: {in_reply_to}
            References: {references if references else 'None'}
            From: {from_addr}
            To: {to_addr}
        """.strip())
        
        # Process email
        with self.conn.cursor() as cur:
            ticket_id = None
            parent_email_id = None
            
            # Search for linked emails
            if in_reply_to or references:
                message_refs = [in_reply_to] + (references if references else [])
                message_refs = [ref for ref in message_refs if ref]
                
                # Search for linked emails
                if message_refs:
                    cur.execute("""
                        SELECT e.ticket_id, e.id, e.message_id
                        FROM emails e
                        WHERE e.message_id = ANY(%s)
                        ORDER BY 
                            CASE WHEN e.message_id = %s THEN 0 ELSE 1 END,
                            e.created_at DESC
                        LIMIT 1
                    """, (message_refs, in_reply_to if in_reply_to else ''))
                    result = cur.fetchone()
                    if result:
                        ticket_id, parent_email_id, parent_message_id = result
        
            # If no thread reference found, search for ticket reference in subject
            if not ticket_id:
                ticket_reference = self._extract_ticket_reference(subject)
                if ticket_reference:
                    cur.execute("""
                        SELECT id FROM tickets WHERE ticket_number = %s
                    """, (ticket_reference,))
                    result = cur.fetchone()
                    if result:
                        ticket_id = result[0]
            
            # Store email
            email_id = str(uuid.uuid4())
            cur.execute("""
                INSERT INTO emails (
                    id, ticket_id, message_id, from_address, 
                    to_address, subject, body, received_at,
                    in_reply_to, references_list
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                email_id, ticket_id, message_id, from_addr,
                to_addr, subject, self._get_email_body(email_message), 
                datetime.now(), in_reply_to, references
            ))

            # Store email thread relationship
            if parent_email_id:
                cur.execute("""
                    INSERT INTO email_threads (parent_email_id, child_email_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, (parent_email_id, email_id))

            # If no ticket found, create new one
            if not ticket_id:
                ticket_id, ticket_number = self._create_new_ticket(cur, subject, from_addr)
                # Update ticket ID for the just stored email
                cur.execute("""
                    UPDATE emails 
                    SET ticket_id = %s 
                    WHERE id = %s
                """, (ticket_id, email_id))
                # Send confirmation email only once
                self._send_confirmation_email(from_addr, ticket_number, subject, ticket_id)

            self.conn.commit()

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
        """Create new ticket with improved ticket number generation"""
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                logger.debug(f"Attempting to create new ticket (attempt {attempt + 1}/{max_attempts})")
                # Determine queue based on email address
                cur.execute("SELECT id, prefix FROM queues LIMIT 1")
                queue_result = cur.fetchone()
                
                if not queue_result:
                    raise ValueError("No queue found in database. At least one queue must exist.")
                
                queue_id, prefix = queue_result

                # Generate next ticket number in a transaction
                cur.execute("BEGIN")
                
                # First get the highest number in a separate query
                cur.execute("""
                    SELECT ticket_number 
                    FROM tickets 
                    WHERE ticket_number LIKE %s 
                    ORDER BY 
                        CAST(SPLIT_PART(ticket_number, '-', 2) AS INTEGER) DESC
                    LIMIT 1
                    FOR UPDATE
                """, (f'{prefix}-%',))
                
                result = cur.fetchone()
                if result:
                    last_num = int(result[0].split('-')[1])
                    new_num = last_num + 1
                else:
                    new_num = 1

                ticket_number = f"{prefix}-{new_num}"
                ticket_id = str(uuid.uuid4())

                # Ticket einfügen
                cur.execute("""
                    INSERT INTO tickets (
                        id, ticket_number, queue_id, subject, 
                        status_name, assigned_supporter_id
                    )
                    VALUES (%s, %s, %s, %s, 'New', NULL)
                """, (ticket_id, ticket_number, str(queue_id), subject))
                
                # Assign supporter
                self._assign_supporter(cur, ticket_id)
                
                # Send confirmation email
                self._send_confirmation_email(from_addr, ticket_number, subject, ticket_id)
                
                cur.execute("COMMIT")
                logger.info(f"Created new ticket: {ticket_number}")
                return ticket_id, ticket_number
                
            except psycopg2.Error as e:
                cur.execute("ROLLBACK")
                if attempt == max_attempts - 1:
                    logger.error(f"Maximum attempts ({max_attempts}) reached: {str(e)}")
                    raise
                logger.warning(f"Attempt {attempt + 1} failed, retrying...")
                time.sleep(0.1 * (attempt + 1))
        
        raise RuntimeError("Failed to create ticket after multiple attempts")

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

    def _send_confirmation_email(self, to_address: str, ticket_number: str, subject: str, ticket_id: str):
        """Send confirmation email to requester and store in DB"""
        smtp = None
        try:
            smtp = smtplib.SMTP_SSL(self.config['email']['smtp_host'])
            smtp.login(self.config['email']['username'], self.config['email']['password'])
            
            # Find reference to original email
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT message_id, in_reply_to, references_list
                    FROM emails 
                    WHERE ticket_id = %s 
                    ORDER BY created_at DESC  -- Letzte E-Mail zuerst
                    LIMIT 1
                """, (ticket_id,))
                original_email = cur.fetchone()
                
            msg = MIMEMultipart()
            msg['From'] = self.config['email']['username']
            msg['To'] = to_address
            msg['Subject'] = f'Ticket created: {ticket_number} - {subject}'
            msg['Message-ID'] = f"<{uuid.uuid4()}@{self.config['email']['smtp_host']}>"
            
            # Set references and In-Reply-To headers
            if original_email and original_email[0]:  # Check if message_id exists
                original_message_id = original_email[0]
                original_in_reply_to = original_email[1]
                original_references = original_email[2] or []
                
                # In-Reply-To is the message-id of the original email
                msg['In-Reply-To'] = f"<{original_message_id}>"
                
                # Build references list
                references = []
                if original_in_reply_to:
                    references.append(original_in_reply_to)
                if original_references:
                    references.extend(original_references)
                if original_message_id:
                    references.append(original_message_id)
                
                if references:
                    msg['References'] = ' '.join(f"<{ref}>" for ref in references)
            
            body = f"""Dear/Ms. Requester,

Thank you for your request. We have created a ticket with the number {ticket_number}.

Subject: {subject}

Please refer to this ticket number in further communication, 
by leaving #{ticket_number} in the subject line.

With kind regards
Your Support Team"""
            
            msg.attach(MIMEText(body.strip(), 'plain'))
            
            # Send email
            smtp.send_message(msg)
            smtp.quit()
            smtp = None
            
            # Store confirmation email in DB
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO emails (
                        id, ticket_id, message_id, from_address, 
                        to_address, subject, body, received_at,
                        in_reply_to, references_list
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    str(uuid.uuid4()), ticket_id, msg['Message-ID'].strip('<>'),
                    self.config['email']['username'], to_address,
                    msg['Subject'], body.strip(), datetime.now(),
                    original_email[0] if original_email else None,
                    references if original_email and references else None
                ))
                self.conn.commit()
            
            logger.info(f"Confirmation email sent for ticket {ticket_number} to {to_address}")
            
        except Exception as e:
            logger.error(f"Failed to send confirmation email: {str(e)}")
            raise
        finally:
            if smtp:
                try:
                    smtp.quit()
                except:
                    logger.warning("Failed to close SMTP connection properly")
                    pass

    def __del__(self):
        self._cleanup()
  