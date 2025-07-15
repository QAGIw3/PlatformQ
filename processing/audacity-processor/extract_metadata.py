import sys
import json
import sqlite3
import xml.etree.ElementTree as ET

def extract_audacity_metadata(filepath):
    """
    Connects to an Audacity project file (.aup3) as an SQLite database
    and extracts project metadata.
    """
    try:
        con = sqlite3.connect(filepath)
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
        
        print(json.dumps(metadata, indent=4))

    except Exception as e:
        error_message = {"error": f"Failed to process Audacity file: {e}"}
        print(json.dumps(error_message))
        sys.exit(1)
    finally:
        if 'con' in locals():
            con.close()

    sys.exit(0)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        error_message = {"error": "No .aup3 file path provided."}
        print(json.dumps(error_message))
        sys.exit(1)
    else:
        extract_audacity_metadata(sys.argv[1]) 