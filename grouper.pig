REGISTER '/home/ivukotic/piggybank-0.14.0.jar' ;

REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jackson-*.jar';
REGISTER '/usr/lib/pig/lib/json-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';


CLEANED = LOAD 'Summary/Cleaned/cleaned.$INPF' as (SRC:chararray,SITE:chararray,TOS:long,TOD:long,TOE:long,IN:long,OUT:long); 

-- CLEANEDL = LIMIT CLEANED 1000; dump CLEANEDL;

-- grouping
grouped = group CLEANED by (SITE, SRC, TOS);
-- l = LIMIT grouped 1; dump l; 

-- sorting
sorted = foreach grouped{ 
    ord = order CLEANED by TOD ASC; 
    generate ord as O; 
    };

STORE sorted into 'Summary/Sorted/sorted.$INPF';
-- l = LIMIT sorted 1; dump l; 

