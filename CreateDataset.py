import glob
import pandas as pd
import lxml.etree as et
from typing import List, Dict
from csv import DictWriter
from tqdm import tqdm


def get_xml_files(path: str) -> List[str]:
    return glob.glob(path + "*.xml")

def _raise_invalid_field_warning(val, field, file):
    if val is None:
        print(f'Error processing {field} in {file}')

def parse_texts(files: str, out_filename: str = "dataset.csv") -> Dict[str, str]:
    """
    TODO Think aboot additional metadate (subtitle, gender, etc.)
    """
    ns = {'tei': 'http://www.tei-c.org/ns/1.0'}
    csv_writer = DictWriter(out_filename, fieldnames=['title', 'author', 'text', 'type'])
    for file in tqdm(files):
        xml = et.parse(file)
        entry = {}
        
        entry['title'] = None

        text = " ".join(" ".join(xml.xpath('//tei:div//text()', namespaces=ns)).split())
        _raise_invalid_field_warning(text, 'text', file)
        entry['text'] = text
