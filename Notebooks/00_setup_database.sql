-- This script sets up the 'Environment' for our project.
-- We use a Catalog to keep everything organized in one place.

CREATE CATALOG IF NOT EXISTS smart_grid;

-- We create three schemas (folders) for our Medallion Architecture.
CREATE SCHEMA IF NOT EXISTS smart_grid.bronze; -- Raw API Data
CREATE SCHEMA IF NOT EXISTS smart_grid.silver; -- Cleaned & Enriched Data
CREATE SCHEMA IF NOT EXISTS smart_grid.gold;   -- Final Business Reports
