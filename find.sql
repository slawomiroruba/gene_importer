SELECT
    COUNT(*)
FROM
    (
    SELECT
        *
    FROM
        marriage_records
    WHERE
        marriage_records.nazwisko_pana LIKE '%Zigan%' OR 
    	marriage_records.nazwisko_pani LIKE '%Zigan%' 
    UNION ALL
SELECT
    *
FROM
    death_records
WHERE
    death_records.nazwisko LIKE '%Zigan%' OR 
    death_records.nazwisko_matki LIKE '%Zigan%'
UNION ALL
SELECT
    *
FROM
    birth_records
WHERE
    birth_records.nazwisko LIKE '%Zigan%' OR 
    birth_records.nazwisko_matki LIKE '%Zigan%'
) AS total