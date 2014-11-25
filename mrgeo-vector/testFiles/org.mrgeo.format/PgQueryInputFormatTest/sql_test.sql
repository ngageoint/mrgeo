select value, ST_ASText(watershed) from t_watershed 
where watershed_id_pk < 3;
