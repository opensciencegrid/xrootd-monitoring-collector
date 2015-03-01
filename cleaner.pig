
REGISTER '/home/ivukotic/piggybank-0.14.0.jar' ;

REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jackson-*.jar';
REGISTER '/usr/lib/pig/lib/json-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';

REGISTER 'cleanup.py' using jython as cleanfuncs;

RAW = LOAD '/user/ivukotic/IlijaCollector/SummaryStream.$INPD/SummaryStream.$INPH.*' as (x:chararray); 

-- RAWL = LIMIT RAW 1000;
-- dump RAWL;

cleaned = foreach RAW generate FLATTEN(cleanfuncs.XMLtoNTUP(x));
--dump cleaned;

STORE cleaned into 'Summary/Cleaned/cleaned.$INPD.$INPH';

-- grouping
grouped = group cleaned by (SITE, SRC, TOS, PID);
-- l = LIMIT grouped 1; dump l; 

-- sorting
sorted = foreach grouped{ 
    ord = order cleaned by TOD ASC; 
    generate ord as O; 
    };

STORE sorted into 'Summary/Sorted/sorted.$INPD.$INPH';
-- l = LIMIT sorted 1; dump l; 
