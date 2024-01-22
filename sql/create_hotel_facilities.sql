CREATE TABLE IF NOT EXISTS hotel_facilities(
    id                               integer PRIMARY KEY,
    property_type                    text,
    room_type                        text,
    accommodates                     integer,
    bathrooms                        numeric,
    bedrooms                         integer,
    beds                             integer,
    bed_type                         text,
    amenities                        text[],
    square_feet                      text
);
CREATE INDEX IF NOT EXISTS airbnb_facilities_index ON hotel_facilities(id);