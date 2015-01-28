#!/usr/bin/python
from datetime import datetime
import xml.etree.ElementTree as ET

@outputSchema("CLEANED:tuple(SRC:chararray,SITE:chararray,TOS:long,TOD:long,IN:long,OUT:long)")
def XMLtoNTUP(xmlInput):
    ntup = []
    root = ET.fromstring(xmlInput)
    print root.attrib
    for child in root:
        print child.tag, child.attrib
        if child.attrib['id']=='link': 
            for c in child:
                if c.tag=='in': r_in=long(c.text)
                if c.tag=='out': r_out=long(c.text)
    return (root.attrib['src'],root.attrib['site'],root.attrib['tos'],root.attrib['tod'],r_in,r_out)
