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
        config.read('config.ini')
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
        """IMAP-Verbindung mit Fehlerbehandlung"""
        mail = None
        try:
            if self.mail:
                try:
                    self.mail.close()
                    self.mail.logout()
                except:
                    pass
                self.mail = None
                gc.collect()  # Speicher freigeben
            
            mail = imaplib.IMAP4_SSL(
                host=self.config['email']['imap_host'],
                timeout=30  # Timeout hinzufügen
            )
            mail.login(self.config['email']['username'], self.config['email']['password'])
            return mail
        except Exception as e:
            logger.error(f"IMAP-Verbindungsfehler: {str(e)}")
            if mail:
                try:
                    mail.logout()
                except:
                    pass
                mail = None
            gc.collect()  # Speicher freigeben
            raise

    def _connect_smtp(self):
        """SMTP-Verbindung aufbauen"""
        if not self.smtp:
            self.smtp = smtplib.SMTP_SSL(self.config['email']['smtp_host'])
            self.smtp.login(self.config['email']['username'], self.config['email']['password'])
        return self.smtp

    def process_emails(self):
        """E-Mails verarbeiten mit verbessertem Ressourcenmanagement"""
        mail = None
        try:
            self.conn = self._connect_db()
            
            # Mehrere Versuche für IMAP-Verbindung
            max_retries = 3
            retry_delay = 5  # Sekunden
            
            for attempt in range(max_retries):
                try:
                    mail = self._connect_imap()
                    break
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    logger.warning(f"Verbindungsversuch {attempt + 1} fehlgeschlagen, warte {retry_delay} Sekunden...")
                    time.sleep(retry_delay)
                    gc.collect()
            
            mail.select('inbox')
            _, messages = mail.search(None, 'UNSEEN')
            
            if not messages[0]:
                return  # Keine neuen Nachrichten

            for message_number in messages[0].split():
                try:
                    _, msg_data = mail.fetch(message_number, '(RFC822)')
                    email_body = msg_data[0][1]
                    email_message = email.message_from_bytes(email_body)
                    
                    try:
                        self._process_single_email(email_message)
                    finally:
                        # Speicher sofort freigeben
                        del email_message
                        del email_body
                        del msg_data
                        gc.collect()
                
                except Exception as e:
                    logger.error(f"Fehler bei der Verarbeitung einer E-Mail: {str(e)}")
                    continue
        finally:
            # Verbindungen sauber schließen
            if mail:
                try:
                    mail.close()
                    mail.logout()
                except:
                    pass
            gc.collect()

    def _cleanup(self):
        """Verbindungen sauber schließen"""
        if self.mail:
            try:
                self.mail.close()
                self.mail.logout()
            except:
                pass
            self.mail = None

        if self.smtp:
            try:
                self.smtp.quit()
            except:
                pass
            self.smtp = None

        if self.conn and not self.conn.closed:
            try:
                self.conn.close()
            except:
                pass
            self.conn = None
        
        gc.collect()

    def _process_single_email(self, email_message):
        subject = email_message.get('subject', '')
        message_id = email_message.get('message-id', '').strip('<>')
        from_addr = parseaddr(email_message.get('from'))[1]
        to_addr = parseaddr(email_message.get('to'))[1]
        
        # E-Mail-Referenzen prüfen und loggen
        in_reply_to = email_message.get('in-reply-to', '').strip('<>')
        references = email_message.get('references', '').split()
        if references:
            references = [ref.strip('<>') for ref in references]
        
        # Header-Informationen loggen
        logger.info(f"""Verarbeite E-Mail:
            Subject: {subject}
            Message-ID: {message_id}
            In-Reply-To: {in_reply_to}
            References: {references if references else 'None'}
            From: {from_addr}
            To: {to_addr}
        """.strip())
        
        with self.conn.cursor() as cur:
            ticket_id = None
            parent_email_id = None
            
            # Nach verknüpften E-Mails suchen
            if in_reply_to or references:
                message_refs = [in_reply_to] + (references if references else [])
                message_refs = [ref for ref in message_refs if ref]
                
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
        
            # Wenn keine Thread-Referenz gefunden, nach Ticket-Referenz im Subject suchen
            if not ticket_id:
                ticket_reference = self._extract_ticket_reference(subject)
                if ticket_reference:
                    cur.execute("""
                        SELECT id FROM tickets WHERE ticket_number = %s
                    """, (ticket_reference,))
                    result = cur.fetchone()
                    if result:
                        ticket_id = result[0]
            
            # E-Mail speichern
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

            # E-Mail-Thread-Beziehung speichern
            if parent_email_id:
                cur.execute("""
                    INSERT INTO email_threads (parent_email_id, child_email_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, (parent_email_id, email_id))

            # Wenn kein Ticket gefunden, neues erstellen
            if not ticket_id:
                ticket_id, ticket_number = self._create_new_ticket(cur, subject, from_addr)
                # Ticket-ID für die gerade gespeicherte E-Mail aktualisieren
                cur.execute("""
                    UPDATE emails 
                    SET ticket_id = %s 
                    WHERE id = %s
                """, (ticket_id, email_id))
                # Bestätigungsmail nur einmal senden
                self._send_confirmation_email(from_addr, ticket_number, subject, ticket_id)

            self.conn.commit()

    def _get_email_body(self, email_message):
        body = ""
        if email_message.is_multipart():
            for part in email_message.walk():
                if part.get_content_type() == "text/plain":
                    body = part.get_payload(decode=True).decode()
                    break
        else:
            body = email_message.get_payload(decode=True).decode()
        return body

    def _extract_ticket_reference(self, subject):
        pattern = r'#([A-Z]+-\d+)'
        match = re.search(pattern, subject)
        return match.group(1) if match else None

    def _create_new_ticket(self, cur, subject, from_addr):
        """Neues Ticket erstellen mit verbesserter Ticketnummer-Generierung"""
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                # Queue basierend auf E-Mail-Adresse bestimmen
                cur.execute("SELECT id, prefix FROM queues LIMIT 1")
                queue_result = cur.fetchone()
                
                if not queue_result:
                    raise ValueError("Keine Queue in der Datenbank gefunden. Mindestens eine Queue muss existieren.")
                
                queue_id, prefix = queue_result

                # Nächste Ticketnummer in einer Transaktion generieren
                cur.execute("BEGIN")
                
                # Zuerst die höchste Nummer in einer separaten Abfrage ermitteln
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
                
                # Supporter zuweisen
                self._assign_supporter(cur, ticket_id)
                
                # Bestätigungsmail senden
                self._send_confirmation_email(from_addr, ticket_number, subject, ticket_id)
                
                cur.execute("COMMIT")
                return ticket_id, ticket_number
                
            except psycopg2.Error as e:
                cur.execute("ROLLBACK")
                if attempt == max_attempts - 1:
                    logger.error(f"Maximale Anzahl von Versuchen ({max_attempts}) erreicht: {str(e)}")
                    raise
                logger.warning(f"Versuch {attempt + 1} fehlgeschlagen, versuche erneut...")
                time.sleep(0.1 * (attempt + 1))  # Exponentielles Backoff
        
        raise RuntimeError("Konnte kein Ticket erstellen nach mehreren Versuchen")

    def _assign_supporter(self, cur, ticket_id):
        # Hole alle aktiven Supporter
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
        """Status eines Tickets aktualisieren"""
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE tickets 
                SET status_name = %s
                WHERE id = %s
            """, (status_name, ticket_id))
            self.conn.commit()

    def _send_confirmation_email(self, to_address: str, ticket_number: str, subject: str, ticket_id: str):
        """Bestätigungsmail an den Requester senden und in DB speichern"""
        smtp = None
        try:
            smtp = smtplib.SMTP_SSL(self.config['email']['smtp_host'])
            smtp.login(self.config['email']['username'], self.config['email']['password'])
            
            # Referenz zur ursprünglichen E-Mail finden
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
            msg['Subject'] = f'Ticket erstellt: {ticket_number} - {subject}'
            msg['Message-ID'] = f"<{uuid.uuid4()}@{self.config['email']['smtp_host']}>"
            
            # References und In-Reply-To Header setzen
            if original_email and original_email[0]:  # Prüfen ob message_id existiert
                original_message_id = original_email[0]
                original_in_reply_to = original_email[1]
                original_references = original_email[2] or []
                
                # In-Reply-To ist die Message-ID der ursprünglichen E-Mail
                msg['In-Reply-To'] = f"<{original_message_id}>"
                
                # References-Liste aufbauen
                references = []
                if original_in_reply_to:
                    references.append(original_in_reply_to)
                if original_references:
                    references.extend(original_references)
                if original_message_id:
                    references.append(original_message_id)
                
                if references:
                    msg['References'] = ' '.join(f"<{ref}>" for ref in references)
            
            body = f"""Sehr geehrte/r Anfragende/r,

vielen Dank für Ihre Anfrage. Wir haben ein Ticket mit der Nummer {ticket_number} erstellt.

Betreff: {subject}

Bitte beziehen Sie sich in weiterer Kommunikation auf diese Ticketnummer, 
indem Sie #{ticket_number} in der Betreffzeile belassen.

Mit freundlichen Grüßen
Ihr Support-Team"""
            
            msg.attach(MIMEText(body.strip(), 'plain'))
            
            # E-Mail senden
            smtp.send_message(msg)
            smtp.quit()
            smtp = None
            
            # Bestätigungsmail in DB speichern
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
            
            logger.info(f"Bestätigungsmail für Ticket {ticket_number} an {to_address} gesendet und gespeichert")
            
        except Exception as e:
            logger.error(f"Fehler beim Senden der Bestätigungsmail: {str(e)}")
            raise
        finally:
            if smtp:
                try:
                    smtp.quit()
                except:
                    pass

    def __del__(self):
        self._cleanup()
  