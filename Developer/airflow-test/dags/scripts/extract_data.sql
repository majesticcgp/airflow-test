COPY (
    SELECT * FROM public.salaries s LIMIT 3000
    )
TO '{{params.employee_salaries}}' WITH (FORMAT CSV, HEADER);
