
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
l = LIMIT gr 1; dump l; 

output = foreach grouped{ 
    sorted = order X by TOD DESC;
    generate group, sorted;
}

l = LIMIT output 1; dump l; 

-- gr = foreach grouped generate FLATTEN(group), X.TOD, X.TOE, X.IN, X.OUT ;

-- l = LIMIT gr 1000; dump l;  

-- sorting

-- 


-- removing already used Maxis
rmf 'Summary/Maxis'

-- creating new Maxis
--STORE NMAXIS into 'Summary/Maxis';




