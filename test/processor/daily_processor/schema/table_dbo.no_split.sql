CREATE TABLE dbo.no_split (
    id INT,
    batch_id INT,
    effort FLOAT,
    started DATETIME2,
    finished DATETIME2,
    overtime BIT,
    status VARCHAR(16),
    deleted BIT
);