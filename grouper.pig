
REGISTER '/home/ivukotic/piggybank-0.14.0.jar' ;

REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jackson-*.jar';
REGISTER '/usr/lib/pig/lib/json-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';

-- REGISTER 'cleanup.py' using jython as cleanfuncs;

CLEANED = LOAD 'Summary/Cleaned/cleaned.$INPF' as (SRC:chararray,SITE:chararray,TOS:long,TOD:long,TOE:long,IN:long,OUT:long); 

-- CLEANEDL = LIMIT CLEANED 1000;
-- dump CLEANEDL;


MAXIS = LOAD 'Summary/Maxis' as (SRC:chararray,SITE:chararray,TOS:long,TOD:long,TOE:long,IN:long,OUT:long); 

X = UNION CLEANED, MAXIS;

-- grouping
grouped = group X by (SITE, SRC, TOS);
l = LIMIT grouped 1; dump l; 

sorted = foreach grouped{ 
    ord = order X by TOD ASC; 
    ma = order X by TOD DESC; 
    mas = LIMIT ma 1; 
    generate group AS g, mas AS gmax, ord; 
    };

l = LIMIT sorted 1; dump l; 


-- gr = foreach grouped generate FLATTEN(group), X.TOD, X.TOE, X.IN, X.OUT ;

-- l = LIMIT gr 1000; dump l;  

-- sorting

-- 


-- removing already used Maxis
rmf 'Summary/Maxis';

-- creating new Maxis
NMAXIS = foreach sorted generate gmax; 
dump NMAXIS;
STORE NMAXIS into 'Summary/Maxis';




