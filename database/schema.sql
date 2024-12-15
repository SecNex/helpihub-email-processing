-- Tabelle für Queues
CREATE TABLE queues (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(100) NOT NULL,
    prefix VARCHAR(10) NOT NULL UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Enum für die Basis-Status
CREATE TYPE ticket_status AS ENUM ('Open', 'Doing', 'Waiting', 'Closed');

-- Tabelle für Status-Definitionen
CREATE TABLE status_definitions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(100) NOT NULL UNIQUE,
    base_status ticket_status NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Beispiel-Status einfügen
INSERT INTO status_definitions (name, base_status, description) VALUES
    ('New', 'Open', 'New ticket'),
    ('Doing', 'Doing', 'Ticket is being worked on'),
    ('Waiting for customer', 'Waiting', 'Waiting for Customer'),
    ('Waiting for external', 'Waiting', 'Waiting for external'),
    ('Waiting for internal', 'Waiting', 'Waiting for internal'),
    ('Waiting for response', 'Waiting', 'Waiting for response'),
    ('Waiting for information', 'Waiting', 'Waiting for information'),
    ('Closed', 'Closed', 'Ticket is closed');

-- Tabelle für Tickets
CREATE TABLE tickets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ticket_number VARCHAR(20) NOT NULL UNIQUE,
    queue_id UUID REFERENCES queues(id),
    subject VARCHAR(255) NOT NULL,
    status_name VARCHAR(100) REFERENCES status_definitions(name),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabelle für E-Mail-Nachrichten
CREATE TABLE emails (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ticket_id UUID REFERENCES tickets(id),
    message_id VARCHAR(255) NOT NULL UNIQUE,
    from_address VARCHAR(255) NOT NULL,
    to_address VARCHAR(255) NOT NULL,
    subject VARCHAR(255) NOT NULL,
    body TEXT,
    received_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    in_reply_to VARCHAR(255),
    references_list TEXT[]
);

-- Tabelle für Supporter
CREATE TABLE supporters (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabelle für Ticket-Supporter-Zuordnung
CREATE TABLE ticket_assignments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ticket_id UUID REFERENCES tickets(id),
    supporter_id UUID REFERENCES supporters(id),
    assigned_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticket_id, supporter_id)
);

-- Spalte für zugewiesenen Supporter in Tickets-Tabelle
ALTER TABLE tickets 
ADD COLUMN assigned_supporter_id UUID REFERENCES supporters(id); 

-- Standard-Queue einfügen
INSERT INTO queues (id, name, prefix) VALUES
    (gen_random_uuid(), 'Default Queue', 'DEF');

-- Tabelle für E-Mail-Thread-Beziehungen
CREATE TABLE email_threads (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    parent_email_id UUID REFERENCES emails(id),
    child_email_id UUID REFERENCES emails(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(parent_email_id, child_email_id)
);