CREATE TABLE dbo.daily_split (
    id INT,
    batch_id INT,
    effort FLOAT,
    started DATETIME2,
    finished DATETIME2,
    overtime BIT,
    status VARCHAR(16),
    deleted BIT,
    started_date DATE,
    finished_date DATE,
);

