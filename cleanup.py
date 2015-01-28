#!/usr/bin/python
from datetime import datetime
import xml.etree.ElementTree as ET

@outputSchema("CLEANED:tuple(SRC:chararray,SITE:chararray,TOS:long,TOD:long,IN:long,OUT:long)")
def XMLtoNTUP(xmlInput):
    ntup = []
    root = ET.fromstring(xmlInput)
    print root.attrib
    return ("host",root['tos'],123,321,111,333)
