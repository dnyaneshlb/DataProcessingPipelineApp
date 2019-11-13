CREATE TABLE public."Orders"
(
    id integer NOT NULL,
    cost integer NOT NULL,
    email character[],
    shipping_address character[] NOT NULL,
    PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE public."Orders"
    OWNER to postgres;