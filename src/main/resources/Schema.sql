-- Table: public."OrderDetails"

-- DROP TABLE public."OrderDetails";

CREATE TABLE public."OrderDetails"
(
    model_number text COLLATE pg_catalog."default",
    cost bigint,
    email text COLLATE pg_catalog."default",
    shipping_address text COLLATE pg_catalog."default",
    mobile_number text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE public."OrderDetails"
    OWNER to postgres;