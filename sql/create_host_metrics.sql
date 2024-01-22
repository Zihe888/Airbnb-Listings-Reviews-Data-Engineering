CREATE TABLE IF NOT EXISTS host_metrics(
    id                               integer PRIMARY KEY,
    requires_license                 boolean,
    license                          text,
    jurisdiction_names               text,
    cancellation_policy              text,
    require_guest_profile_picture    boolean,
    require_guest_phone_verification boolean,
    calculated_host_listings_count   integer,
    reviews_per_month                numeric
);
CREATE INDEX IF NOT EXISTS airbnb_metrics_index ON host_metrics (id);