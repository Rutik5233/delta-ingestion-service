CREATE TABLE countries (
                           id   BIGSERIAL PRIMARY KEY,
                           code TEXT UNIQUE NOT NULL,
                           name TEXT NOT NULL
);

CREATE TABLE customer_status (
                                 id   BIGSERIAL PRIMARY KEY,
                                 code TEXT UNIQUE NOT NULL,
                                 name TEXT NOT NULL
);