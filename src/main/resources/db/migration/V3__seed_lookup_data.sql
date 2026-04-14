INSERT INTO countries (code, name) VALUES
                                       ('US', 'United States'),
                                       ('IN', 'India'),
                                       ('GB', 'United Kingdom'),
                                       ('DE', 'Germany'),
                                       ('CA', 'Canada'),
                                       ('AU', 'Australia'),
                                       ('SG', 'Singapore'),
                                       ('JP', 'Japan')
    ON CONFLICT (code) DO NOTHING;

INSERT INTO customer_status (code, name) VALUES
                                             ('ACTIVE',    'Active'),
                                             ('INACTIVE',  'Inactive'),
                                             ('SUSPENDED', 'Suspended'),
                                             ('PENDING',   'Pending Activation')
    ON CONFLICT (code) DO NOTHING;