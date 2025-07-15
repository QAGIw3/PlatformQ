import sys
import json
import sqlite3
import xml.etree.ElementTree as ET
from platformq_shared.processor_utils import run_processor

def extract_audacity_metadata(filepath):
    """
    Connects to an Audacity project file (.aup3) as an SQLite database
    and extracts project metadata.
    """
    con = sqlite3.connect(filepath)
    try:
        cur = con.cursor()
        
        # The main project metadata is in the 'project' table
        res = cur.execute("SELECT rate, xml FROM project")
        project_data = res.fetchone()
        
        if not project_data:
            raise ValueError("Could not find project data in the .aup3 file.")

        sample_rate = project_data[0]
        xml_data = project_data[1]
        
        # The track names are embedded in the XML data
        root = ET.fromstring(xml_data)
        track_names = [wavetrack.get('name') for wavetrack in root.findall('{http://audacity.sourceforge.net/xml/}wavetrack')]

        metadata = {
            "audacity_project": {
                "sample_rate": sample_rate,
                "track_count": len(track_names),
                "track_names": track_names,
            }
        }
        
        return metadata

    finally:
        if 'con' in locals():
            con.close()

if __name__ == '__main__':
    run_processor(extract_audacity_metadata) 