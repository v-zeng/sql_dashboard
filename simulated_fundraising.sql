-- set database
USE DonorAnalytics

-- create database
CREATE DATABASE DonorAnalytics;
GO
USE DonorAnalytics;

-- Campaigns (reference table)
CREATE TABLE Campaigns (
	campaign_id VARCHAR(36) PRIMARY KEY, -- UUID for compaign
	campaign_name VARCHAR(100) UNIQUE,
	campaign_type VARCHAR(50),
	platform_used VARCHAR(50),
	campaign_start_date DATE,
	campaign_end_date DATE
);

-- Donors (main fundraising data)
CREATE TABLE Donors (
	donor_id VARCHAR(36),
	donor_age INT,
	donor_gender VARCHAR(10),
	donor_location VARCHAR(100),
	donor_type VARCHAR(50), -- 'Corporate' or 'Individual'
	donation_date DATE,
	donation_amount DECIMAL(10,2),
	donation_method VARCHAR(50),
	campaign VARCHAR(100),
	campaign_id VARCHAR(36), -- references Campaigns table
	repeat_donor BIT, -- 0=first-time, 1=repeat
	PRIMARY KEY (donor_id, donation_date), -- composite primary key due to repeat donors having same donor_id
	FOREIGN KEY (campaign_id) REFERENCES Campaigns(campaign_id)
);

-- import data into tables
BULK INSERT Campaigns
FROM "C:\Users\Vins\Desktop\SQL\data\simulated_campaigns_data.csv"
WITH (
	FORMAT='CSV',
	FIRSTROW=2,
	FIELDTERMINATOR=',',
	ROWTERMINATOR='\n',
	TABLOCK
);

BULK INSERT Donors --need to fix donation_date format, add 'campaign' column
FROM "C:\Users\Vins\Desktop\SQL\data\cleaned_simulated_fundraising_data.csv"
WITH (
	FORMAT='CSV',
	FIRSTROW=2,
	FIELDTERMINATOR=',',
	ROWTERMINATOR='\n',
	TABLOCK
);

-- add non-clustered indexes on donation_date and campaign_id
CREATE NONCLUSTERED INDEX idx_donation_date ON Donors(donation_date);
CREATE NONCLUSTERED INDEX idx_campaign_id ON Donors(campaign_id);
CREATE NONCLUSTERED INDEX idx_campaign_name ON Campaigns(campaign_name);
CREATE NONCLUSTERED INDEX idx_donor_id ON Donors(donor_id);

-- verify data import for Donors table
SELECT TOP 10 * 
FROM Donors;

-- verify data import for Campaigns table
SELECT * 
FROM Campaigns

-- total donations by donor
SELECT donor_id, donor_type, SUM(donation_amount) AS total_donations
FROM Donors
GROUP BY donor_id, donor_type
ORDER BY total_donations DESC;

-- total donations by campaign
SELECT campaign, SUM(donation_amount) AS total_donations
FROM Donors
GROUP BY campaign
ORDER BY total_donations DESC;

-- repeat donor count
SELECT COUNT(DISTINCT donor_id) AS repeat_donors
FROM Donors
WHERE repeat_donor = 1;

-- first time donor count
SELECT COUNT(DISTINCT donor_id) AS first_time_donors
FROM Donors
WHERE repeat_donor = 0;

-- summarize total donations per donor and rank based on total contributions
SELECT donor_id,
	SUM(donation_amount) AS total_donations,
	COUNT(donation_amount) AS donation_count,
	RANK() OVER (ORDER BY SUM(donation_amount) DESC) AS donor_rank
FROM Donors
GROUP BY donor_id
ORDER BY total_donations DESC;

-- donations by campaign and donor type
SELECT campaign, donor_type, SUM(donation_amount) AS total_donations
FROM Donors
GROUP BY campaign, donor_type
ORDER BY total_donations DESC;

-- monthly donation trends
SELECT
	FORMAT(donation_date, 'yyyy-MM') AS donation_month,
	SUM(donation_amount) AS total_donations,
	COUNT(donor_id) AS donation_count
FROM Donors
GROUP BY FORMAT(donation_date, 'yyyy-MM')
ORDER BY donation_month;

-- running total (cumulative donations)
SELECT donor_id, donation_date, donation_amount,
	SUM(donation_amount) OVER (PARTITION BY donor_id ORDER BY donation_date) AS running_total
FROM Donors
ORDER BY donor_id, donation_date;

-- donor retention by year
WITH YearlyDonors AS (
	SELECT donor_id, YEAR(donation_date) AS donation_year
	FROM Donors
	GROUP BY donor_id, YEAR(donation_date)
)
SELECT d1.donation_year AS first_year,
	COUNT(DISTINCT d1.donor_id) AS new_donors,
	COUNT(DISTINCT d2.donor_id) AS retained_donors
FROM YearlyDonors d1
LEFT JOIN YearlyDonors d2
	ON d1.donor_id = d2.donor_id
	AND d1.donation_year = d2.donation_year - 1
GROUP BY d1.donation_year
ORDER BY d1.donation_year;

-- donors who increased their contributions (year-over-year growth)
WITH YearlyDonations AS (
	SELECT donor_id, YEAR(donation_date) AS donation_year, SUM(donation_amount) AS total_donations
	FROM Donors
	GROUP BY donor_id, YEAR(donation_date)
)
SELECT
	yd1.donor_id,
	yd1.donation_year AS previous_year,
	yd1.total_donations AS prev_donations,
	yd2.donation_year AS current_year,
	yd2.total_donations AS current_donations,
	yd2.total_donations - yd1.total_donations AS donation_growth
FROM YearlyDonations yd1
JOIN YearlyDonations yd2
	ON yd1.donor_id = yd2.donor_id
	AND yd1.donation_year = yd2.donation_year - 1
WHERE yd2.total_donations > yd1.total_donations
ORDER BY donation_growth DESC;

-- most impactful fundraising campaigns by average donation size
SELECT campaign,
	COUNT(*) AS donation_count,
	SUM(donation_amount) AS total_donations,
	AVG(donation_amount) AS avg_donation
FROM Donors
GROUP BY campaign
ORDER BY avg_donation DESC;

-- create views for Power BI=============

-- donor summary
CREATE VIEW v_DonorSummary AS
SELECT 
	donor_id, 
	donor_type, 
	SUM(donation_amount) AS total_donations,
	COUNT(donation_date) AS donation_count
FROM Donors
GROUP BY donor_id, donor_type;

-- campaign performance
CREATE VIEW v_CampaignPerformance AS
SELECT
	campaign,
	count(*) AS donation_count,
	SUM(donation_amount) AS total_donations,
	AVG(donation_amount) AS avg_donation
FROM Donors
GROUP BY campaign;

-- monthly donation trends
CREATE VIEW v_MonthlyDonationTrends AS
SELECT
	FORMAT(donation_date, 'yyyy-MM') AS donation_month,
	SUM(donation_amount) AS total_donations,
	COUNT(donor_id) AS donation_count
FROM Donors
GROUP BY FORMAT(donation_date, 'yyyy-MM');

-- donor retention (new vs returning donors by year)
CREATE OR ALTER VIEW v_DonorRetention AS
WITH YearlyDonors AS (
    SELECT DISTINCT donor_id, YEAR(donation_date) AS donation_year
    FROM Donors
)
SELECT
    d1.donation_year AS donation_year,
    COUNT(DISTINCT d1.donor_id) AS new_donors,
    COUNT(DISTINCT d2.donor_id) AS retained_donors,
    CASE
        WHEN COUNT(DISTINCT d1.donor_id) = 0 THEN NULL
        ELSE CAST(COUNT(DISTINCT d2.donor_id) AS FLOAT) / COUNT(DISTINCT d1.donor_id)
    END AS retention_rate
FROM YearlyDonors d1
LEFT JOIN YearlyDonors d2
    ON d1.donor_id = d2.donor_id
    AND d1.donation_year = d2.donation_year - 1
WHERE d1.donation_year IN (2023, 2024)  -- Filter to include only 2023 and 2024
GROUP BY d1.donation_year;

-- donor size categories
CREATE OR ALTER VIEW v_DonorSizeCategories AS
SELECT
	donor_id,
	donor_type,
	SUM(donation_amount) AS total_donations,
	COUNT(donation_date) AS donation_count,
	CASE
		WHEN SUM(donation_amount) >= 0 AND SUM(donation_amount) <= 1000 THEN 'Supporter'
		WHEN SUM(donation_amount) > 1000 AND SUM(donation_amount) <= 5000 THEN 'Hero'
		WHEN SUM(donation_amount) > 10000 THEN 'Elite'
    ELSE 'Unknown'
	END AS donor_size_category
FROM Donors
GROUP BY donor_id, donor_type;



-- create stored procedures for dynamic reports=========

-- get donations within date range
CREATE PROCEDURE GetDonationsByDateRange
	@StartDate DATE,
	@EndDate DATE
AS
BEGIN
	SELECT donor_id, donation_date, donation_amount, campaign
	FROM Donors
	WHERE donation_date BETWEEN @StartDate AND @EndDate;
END;

-- donors who increased contributions (YoY growth)
CREATE PROCEDURE GetGrowingDonors AS
BEGIN
	WITH YearlyDonations AS(
		SELECT donor_id,
			YEAR(donation_date) AS donation_year,
			SUM(donation_amount) AS total_donations
		FROM Donors
		GROUP BY donor_id, YEAR(donation_date)
	)
	SELECT
		yd1.donor_id,
		yd1.donation_year AS previous_year,
		yd1.total_donations AS prev_donations,
		yd2.donation_year AS current_year,
		yd2.total_donations AS current_donations,
		yd2.total_donations - yd1.total_donations AS donation_growth
	FROM YearlyDonations yd1
	JOIN YearlyDonations yd2 ON yd1.donor_id = yd2.donor_id
	AND yd1.donation_year = yd2.donation_year - 1
	WHERE yd2.total_donations > yd1.total_donations
	ORDER BY donation_growth DESC;
END;


-- campaign performances
CREATE PROCEDURE GetCampaignPerformance @CampaignName VARCHAR(100)
AS
BEGIN
	SELECT
		campaign,
		COUNT(*) AS donation_count,
		SUM(donation_amount) AS total_donations,
		AVG(donation_amount) AS avg_donation
	FROM Donors
	WHERE campaign = @CampaignName
	GROUP BY campaign;
END;

-- donor contributions over time
CREATE PROCEDURE GetDonorTrend @DonorID VARCHAR(36)
AS
BEGIN
	SELECT
		YEAR(donation_date) AS donation_year,
		SUM(donation_amount) AS total_donations,
		COUNT(*) AS donation_count
	FROM Donors
	WHERE donor_id = @DonorID
	GROUP BY YEAR(donation_date)
	ORDER BY donation_year;
END;