SELECT univiersities AS university, carrera AS career,
    inscription_dates AS inscription_date,
    split_part("names", '-', 1) AS first_name,
    split_part("names", '-', 2) AS last_name,
    sexo AS gender,
        case 
            when 
                left(fechas_nacimiento, 2)::int > 22 
            then 
                date_part('year', age(to_date(concat('19', fechas_nacimiento), 'YYYY/Mon/DD')))::int
            else
                date_part('year', age(to_date(concat('20', fechas_nacimiento), 'YYYY/Mon/DD')))::int
        end as age,
    localidad AS location, email
FROM rio_cuarto_interamericana
WHERE univiersities='-universidad-abierta-interamericana'
AND to_date(inscription_dates, 'YY/Mon/DD')
BETWEEN '2020-Sep-01'::date AND '2021-Feb-01'::date;