
CREATE CATALOG IF NOT EXISTS indicium_dev;

CREATE SCHEMA IF NOT EXISTS indicium_dev.bronze;
CREATE SCHEMA IF NOT EXISTS indicium_dev.silver;
CREATE SCHEMA IF NOT EXISTS indicium_dev.gold;


CREATE VOLUME IF NOT EXISTS indicium_dev.bronze.landing_erp;


CREATE CATALOG IF NOT EXISTS indicium_prod;

CREATE SCHEMA IF NOT EXISTS indicium_prod.bronze;
CREATE SCHEMA IF NOT EXISTS indicium_prod.silver;
CREATE SCHEMA IF NOT EXISTS indicium_prod.gold;


CREATE VOLUME IF NOT EXISTS indicium_prod.bronze.landing_erp;