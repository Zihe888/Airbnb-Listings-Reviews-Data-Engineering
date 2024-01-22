CREATE TABLE IF NOT EXISTS price_info(
    id                               integer PRIMARY KEY,
    price                            text,
    weekly_price                     text,
    monthly_price                    text,
    security_deposit                 text,
    cleaning_fee                     text,
    guests_included                  integer,
    extra_people                     text, 
    minimum_nights                   integer,
    maximum_nights                   integer,
    calendar_updated                 text,
    availability_30                  integer,
    availability_60                  integer,
    availability_90                  integer,
    availability_365                 integer
);
CREATE INDEX IF NOT EXISTS airbnb_price_index ON price_info(id);