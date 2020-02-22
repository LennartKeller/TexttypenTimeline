import glob
import lxml.etree as et
from typing import List, Dict
from csv import DictWriter
from tqdm import tqdm
import os

def get_xml_files(path: str) -> List[str]:
    if path.endswith('/'):
        path = path.rstrip('/')
    return glob.glob(path + "/*.xml")

def _raise_invalid_field_warning(val, field, file):
    if not val:
        print(f'Error processing {field} in {file}')

def parse_texts(files: str, out_filename: str = "dataset.csv") -> Dict[str, str]:
    """
    TODO Think aboot additional metadate (subtitle, gender, etc.)
    """
    ns = {'tei': 'http://www.tei-c.org/ns/1.0'}
    out_file = open(out_filename, 'a', encoding='UTF-8')
    csv_writer = DictWriter(out_file, fieldnames=['file', 'title', 'author', 'text', 'main_type', 'sub_type'])
    csv_writer.writeheader()
    
    for file in tqdm(files, desc='Processing XML-Files ...'):
        xml = et.parse(file)
        entry = {'file': os.path.basename(file)}
        
        title = xml.xpath('//tei:titleStmt/tei:title[@type="main"]/text()', namespaces=ns)[0]
        #_raise_invalid_field_warning(title, 'title', file)
        entry['title'] = title

        author_surname = list(set(xml.xpath('//tei:author[1]/tei:persName/tei:surname/text()', namespaces=ns)))
        _raise_invalid_field_warning(author_surname, 'author_surname', file)
        author_forename = list(set(xml.xpath('//tei:author[1]/tei:persName/tei:forename/text()', namespaces=ns)))
        _raise_invalid_field_warning(author_surname, 'author_forename', file)

        entry['author'] = " ".join(author_forename + author_surname)

        text = " ".join(" ".join(xml.xpath('//tei:div//text()', namespaces=ns)).split())
        _raise_invalid_field_warning(text, 'text', file)
        entry['text'] = text

        main_type =  " ".join(xml.xpath('//tei:textClass/tei:classCode[@scheme="http://www.deutschestextarchiv.de/doku/klassifikation#dtamain"]/text()', namespaces=ns))
        _raise_invalid_field_warning(main_type, 'main_type', file)
        entry['main_type'] = main_type

        sub_type =  " ".join(xml.xpath('//tei:textClass/tei:classCode[@scheme="http://www.deutschestextarchiv.de/doku/klassifikation#dtasub"]/text()', namespaces=ns))
        _raise_invalid_field_warning(sub_type, 'sub_type', file)
        entry['sub_type'] = sub_type

        csv_writer.writerow(entry)
    return

if __name__ == '__main__':
    files = get_xml_files('korpus')
    print(len(files))
    parse_texts(files)
