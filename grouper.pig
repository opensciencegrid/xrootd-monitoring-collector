
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

cleaned = foreach RAW generate FLATTEN(cleanfuncs.XMLtoNTUP(x));
--dump cleaned;

STORE cleaned into 'Summary/Cleaned/cleaned.$INPF';

MAXIS = LOAD 'Summary/Maxis' as (SRC:chararray,SITE:chararray,TOS:long,TOD:long,TOE:long,IN:long,OUT:long); 

X = UNION CLEANED, MAXIS;

-- grouping
grouped = group X by (SITE, SRC, TOS);
gr = foreach grouped generate FLATTEN(group), X.TOD, X.TOE, X.IN, X.OUT ;

-- l = LIMIT gr 1000;
-- dump l;  

-- sorting

-- 

-- creating new Maxis

-- removing already used Maxis
rmf 'Summary/Maxis'



