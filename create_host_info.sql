CREATE TABLE IF NOT EXISTS host_info(
    id                               integer PRIMARY KEY,
    host_id                          integer,
    host_url                         text,
    host_name                        varchar(128),
    host_since                       text,
    host_location                    varchar(256),
    host_response_time               text,
    host_response_rate               text,
    host_acceptance_rate             text,
    host_neighbourhood               text,
    host_listings_count              integer,
    host_total_listings_count        integer,
    host_verifications               text
);
CREATE INDEX IF NOT EXISTS airbnb_index ON host_info(id);