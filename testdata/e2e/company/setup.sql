-- Company scenario: 12 tables with FK constraints and seed data

CREATE TABLE company (
    id   BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE tag (
    id   BIGINT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE company_tag (
    company_id BIGINT NOT NULL REFERENCES company(id),
    tag_id     BIGINT NOT NULL REFERENCES tag(id),
    PRIMARY KEY (company_id, tag_id)
);

CREATE TABLE website (
    id         BIGINT PRIMARY KEY,
    company_id BIGINT NOT NULL REFERENCES company(id),
    url        TEXT NOT NULL
);

CREATE TABLE website_description (
    id         BIGINT PRIMARY KEY,
    website_id BIGINT NOT NULL REFERENCES website(id),
    description TEXT NOT NULL
);

CREATE TABLE website_tag (
    website_id BIGINT NOT NULL REFERENCES website(id),
    tag_id     BIGINT NOT NULL REFERENCES tag(id),
    PRIMARY KEY (website_id, tag_id)
);

CREATE TABLE profile (
    id         BIGINT PRIMARY KEY,
    company_id BIGINT NOT NULL REFERENCES company(id),
    bio        TEXT NOT NULL
);

CREATE TABLE profile_ftes (
    id         BIGINT PRIMARY KEY,
    profile_id BIGINT NOT NULL REFERENCES profile(id),
    count      INT NOT NULL
);

CREATE TABLE profile_tag (
    profile_id BIGINT NOT NULL REFERENCES profile(id),
    tag_id     BIGINT NOT NULL REFERENCES tag(id),
    PRIMARY KEY (profile_id, tag_id)
);

CREATE TABLE legal_entity (
    id         BIGINT PRIMARY KEY,
    company_id BIGINT NOT NULL REFERENCES company(id),
    name       TEXT NOT NULL
);

CREATE TABLE legal_entity_financial (
    id              BIGINT PRIMARY KEY,
    legal_entity_id BIGINT NOT NULL REFERENCES legal_entity(id),
    revenue         BIGINT NOT NULL
);

CREATE TABLE legal_entity_tag (
    legal_entity_id BIGINT NOT NULL REFERENCES legal_entity(id),
    tag_id          BIGINT NOT NULL REFERENCES tag(id),
    PRIMARY KEY (legal_entity_id, tag_id)
);

-- Seed data

INSERT INTO company (id, name, created_at) VALUES
    (1, 'Acme Corp', '2024-01-01 00:00:00'),
    (2, 'Globex Inc', '2024-02-01 00:00:00'),
    (3, 'Initech', '2024-03-01 00:00:00');

INSERT INTO tag (id, name) VALUES
    (1, 'technology'),
    (2, 'finance'),
    (3, 'healthcare');

INSERT INTO company_tag (company_id, tag_id) VALUES
    (1, 1), (1, 2),
    (2, 1),
    (3, 3);

INSERT INTO website (id, company_id, url) VALUES
    (1, 1, 'https://acme.example.com'),
    (2, 2, 'https://globex.example.com'),
    (3, 1, 'https://acme-blog.example.com');

INSERT INTO website_description (id, website_id, description) VALUES
    (1, 1, 'Main Acme website'),
    (2, 2, 'Globex corporate site');

INSERT INTO website_tag (website_id, tag_id) VALUES
    (1, 1), (2, 1), (3, 2);

INSERT INTO profile (id, company_id, bio) VALUES
    (1, 1, 'Leading innovator in tech'),
    (2, 2, 'Global exports specialist'),
    (3, 3, 'Enterprise solutions provider');

INSERT INTO profile_ftes (id, profile_id, count) VALUES
    (1, 1, 500),
    (2, 2, 1200),
    (3, 3, 300);

INSERT INTO profile_tag (profile_id, tag_id) VALUES
    (1, 1), (2, 1), (3, 3);

INSERT INTO legal_entity (id, company_id, name) VALUES
    (1, 1, 'Acme Corp LLC'),
    (2, 1, 'Acme Europe GmbH'),
    (3, 2, 'Globex Holdings Ltd');

INSERT INTO legal_entity_financial (id, legal_entity_id, revenue) VALUES
    (1, 1, 5000000),
    (2, 2, 1200000),
    (3, 3, 8000000);

INSERT INTO legal_entity_tag (legal_entity_id, tag_id) VALUES
    (1, 1), (1, 2), (3, 1);
