-- QUERY 1: Show the top 5 cheapest hours to run the factory.
-- This is what our Gold layer was built for!
SELECT * FROM smart_grid.gold.factory_schedule 
ORDER BY rank ASC;

-- QUERY 2: Find 'Eco-Friendly' windows.
-- Let's look for times where the Eco Score is high (above 50).
SELECT time_window, energy_cost, eco_score
FROM smart_grid.silver.refined_energy_data
WHERE eco_score > 50
ORDER BY eco_score DESC;

-- QUERY 3: Calculate the average cost for the next 24 hours.
-- Useful for budgeting!
SELECT round(avg(energy_cost), 2) as average_daily_price
FROM smart_grid.silver.refined_energy_data;
