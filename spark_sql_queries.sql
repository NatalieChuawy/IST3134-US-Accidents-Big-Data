-- Run after clean DataFrame is registered as temp view: accidents
SELECT State, COUNT(*) AS Accident_Count FROM accidents WHERE State IS NOT NULL GROUP BY State ORDER BY Accident_Count DESC LIMIT 10;
SELECT City, State, COUNT(*) AS Accident_Count FROM accidents WHERE City IS NOT NULL AND State IS NOT NULL GROUP BY City, State ORDER BY Accident_Count DESC LIMIT 10;
SELECT Hour, COUNT(*) AS Accident_Count FROM accidents WHERE Hour IS NOT NULL GROUP BY Hour ORDER BY Hour;
SELECT Weather_Condition, COUNT(*) AS Accident_Count FROM accidents WHERE Weather_Condition IS NOT NULL GROUP BY Weather_Condition ORDER BY Accident_Count DESC LIMIT 10;
SELECT Severity, COUNT(*) AS Accident_Count FROM accidents GROUP BY Severity ORDER BY Severity;
SELECT Sunrise_Sunset, COUNT(*) AS Accident_Count FROM accidents WHERE Sunrise_Sunset IN ('Day','Night') GROUP BY Sunrise_Sunset ORDER BY Accident_Count DESC;
