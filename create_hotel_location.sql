CREATE TABLE IF NOT EXISTS hotel_location(
    id                               integer PRIMARY KEY,
    street                           text         not null,
    neighborhood                     text,
    city                             text,
    state                            char(2)      not null,
    zipcode                          integer,
    market                           text,
    smart_location                   text,
    latitude                         numeric,
    longitude                        numeric
);
CREATE INDEX IF NOT EXISTS airbnb_location_index ON hotel_location(id);