SELECT mc.movie_id as movie_id,
    mc.company_id as company,
    akt.title as title
FROM movie_companies as mc,
    title as tt,
    aka_title as akt
WHERE mc.movie_id IN (SELECT akt.movie_id from akt.aka_title HAVING COUNT(*) > 0)
AND mc.movie_id = tt.id
AND tt.id = akt.movie_id;