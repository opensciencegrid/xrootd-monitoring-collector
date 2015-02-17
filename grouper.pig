REGISTER '/home/ivukotic/piggybank-0.14.0.jar' ;

REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jackson-*.jar';
REGISTER '/usr/lib/pig/lib/json-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';


CLEANED = LOAD 'Summary/Cleaned/cleaned.$INPF' as (SRC:chararray,SITE:chararray,TOS:long,TOD:long,TOE:long,IN:long,OUT:long); 

-- CLEANEDL = LIMIT CLEANED 1000;
-- dump CLEANEDL;


MAXIS = LOAD '/user/ivukotic/Summary/Maxis' as (SRC:chararray,SITE:chararray,TOS:long,TOD:long,TOE:long,IN:long,OUT:long); 

X = UNION CLEANED, MAXIS;

-- grouping
grouped = group X by (SITE, SRC, TOS);
--l = LIMIT grouped 1; dump l; 

sorted = foreach grouped{ 
    ord = order X by TOD ASC; 
    ma  = order X by TOD DESC; 
    --mas = LIMIT ma 1; 
    fmas = TOP(1,4,ma);
    generate group AS g, fmas AS gmax, ord as O; 
    };

DESCRIBE sorted;

l = LIMIT sorted 1; dump l; 


-- sorting




-- creating new Maxis
NoviMAXIS = foreach sorted generate gmax; 
dump NoviMAXIS;
-- STORE NMAXIS into '/user/ivukotic/Summary/MaxisNEW';

