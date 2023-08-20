DROP VIEW if exists ${schema:raw}.population_total CASCADE;

CREATE OR REPLACE VIEW ${schema:raw}.population_total AS
	SELECT SUM((item->>'Population')::INT) AS total_population 
    FROM (
        SELECT jsonb_array_elements(doc_record->'data')::jsonb AS item 
        FROM ${schema:raw}.api_data
    ) AS jsonItems 
    WHERE (item->>'ID Year')::INT IN (2020, 2019, 2018);

