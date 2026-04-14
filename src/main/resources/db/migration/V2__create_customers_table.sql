CREATE TABLE customers (
                           customer_id BIGSERIAL PRIMARY KEY,
                           external_id TEXT      NOT NULL,
                           name        TEXT      NOT NULL,
                           email       TEXT      NOT NULL,
                           country_id  BIGINT    NOT NULL REFERENCES countries(id),
                           status_id   BIGINT    NOT NULL REFERENCES customer_status(id),
                           created_at  TIMESTAMP NOT NULL DEFAULT NOW(),

                           CONSTRAINT uq_customers_external_id UNIQUE (external_id)
);

CREATE INDEX idx_customers_external_id ON customers(external_id);