CREATE TABLE IF NOT EXISTS DEAL (
    TRADENO BIGINT,
    TRADEDATE VARCHAR (10),
    TRADETIME VARCHAR (8),
    SECID VARCHAR (20),
    BOARDID VARCHAR (4),
    PRICE NUMERIC (10, 2),
    QUANTITY INT,
    VALUE NUMERIC (10, 2),
    TYPE VARCHAR (20),
    BUYSELL VARCHAR (1),
    TRADINGSESSION VARCHAR (20),
    PRIMARY KEY (TRADENO)
);

GRANT ALL PRIVILEGES ON DATABASE tradesdb to trades_user;
GRANT ALL PRIVILEGES ON TABLE DEAL TO trades_user;

COPY DEAL(TRADENO, TRADEDATE, TRADETIME, SECID, BOARDID, PRICE, QUANTITY, VALUE, TYPE, BUYSELL, TRADINGSESSION)
FROM '/docker-entrypoint-initdb.d/trades_small.csv'
DELIMITER ';'
CSV HEADER;