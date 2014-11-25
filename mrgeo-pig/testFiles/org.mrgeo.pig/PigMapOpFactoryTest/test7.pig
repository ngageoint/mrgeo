roads = LOAD '{%1}' USING MrGeoLoader;
result = FOREACH roads GENERATE NAME, HIGHWAY,
(MAXSPEED == '' ? 
  (HIGHWAY == 'primary' ? 45 :
  (HIGHWAY == 'secondary' ? 40 : 25))
  : (int)MAXSPEED) as MAXSPEED:int,
GEOMETRY;
