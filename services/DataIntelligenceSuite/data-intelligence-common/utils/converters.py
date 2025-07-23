"""
Data format converters.

Provides conversion utilities for various data formats and types.
"""

import json
import csv
import io
import base64
from typing import Any, Dict, List, Optional, Union, Type, Callable
from datetime import datetime, date, time, timedelta
from decimal import Decimal
from enum import Enum
import pandas as pd
import numpy as np
from dataclasses import dataclass, asdict, is_dataclass
import yaml
import xml.etree.ElementTree as ET
from xml.dom import minidom

from ..monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class DataFormat(str, Enum):
    """Supported data formats"""
    JSON = "json"
    CSV = "csv"
    YAML = "yaml"
    XML = "xml"
    PARQUET = "parquet"
    AVRO = "avro"
    MSGPACK = "msgpack"
    PICKLE = "pickle"


class ConversionError(Exception):
    """Data conversion error"""
    pass


class TypeConverter:
    """Type conversion utilities"""
    
    @staticmethod
    def to_json_serializable(obj: Any) -> Any:
        """Convert object to JSON serializable format"""
        if isinstance(obj, (str, int, float, bool, type(None))):
            return obj
        elif isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, time):
            return obj.strftime("%H:%M:%S")
        elif isinstance(obj, timedelta):
            return obj.total_seconds()
        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, bytes):
            return base64.b64encode(obj).decode('utf-8')
        elif isinstance(obj, Enum):
            return obj.value
        elif is_dataclass(obj):
            return asdict(obj)
        elif hasattr(obj, '__dict__'):
            return obj.__dict__
        elif isinstance(obj, (list, tuple)):
            return [TypeConverter.to_json_serializable(item) for item in obj]
        elif isinstance(obj, dict):
            return {
                key: TypeConverter.to_json_serializable(value)
                for key, value in obj.items()
            }
        elif isinstance(obj, set):
            return list(obj)
        elif hasattr(obj, 'to_dict'):
            return obj.to_dict()
        else:
            return str(obj)
            
    @staticmethod
    def from_json_string(
        json_str: str,
        target_type: Optional[Type] = None
    ) -> Any:
        """Convert JSON string to object"""
        data = json.loads(json_str)
        
        if target_type:
            return TypeConverter.convert_to_type(data, target_type)
        return data
        
    @staticmethod
    def convert_to_type(value: Any, target_type: Type) -> Any:
        """Convert value to target type"""
        if value is None:
            return None
            
        if isinstance(value, target_type):
            return value
            
        # Handle common conversions
        if target_type == str:
            return str(value)
        elif target_type == int:
            return int(value)
        elif target_type == float:
            return float(value)
        elif target_type == bool:
            if isinstance(value, str):
                return value.lower() in ['true', '1', 'yes', 'on']
            return bool(value)
        elif target_type == datetime:
            if isinstance(value, str):
                return datetime.fromisoformat(value.replace('Z', '+00:00'))
            elif isinstance(value, (int, float)):
                return datetime.fromtimestamp(value)
        elif target_type == date:
            if isinstance(value, str):
                return date.fromisoformat(value)
            elif isinstance(value, datetime):
                return value.date()
        elif target_type == Decimal:
            return Decimal(str(value))
        elif target_type == bytes:
            if isinstance(value, str):
                return base64.b64decode(value)
            return bytes(value)
        else:
            # Try constructor
            return target_type(value)


class DataFrameConverter:
    """DataFrame conversion utilities"""
    
    @staticmethod
    def to_dict_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Convert DataFrame to list of dictionaries"""
        # Handle datetime columns
        df_copy = df.copy()
        for col in df_copy.columns:
            if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
                df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                
        return df_copy.to_dict('records')
        
    @staticmethod
    def from_dict_records(
        records: List[Dict[str, Any]],
        dtypes: Optional[Dict[str, Type]] = None
    ) -> pd.DataFrame:
        """Create DataFrame from list of dictionaries"""
        df = pd.DataFrame(records)
        
        # Apply dtypes if provided
        if dtypes:
            for col, dtype in dtypes.items():
                if col in df.columns:
                    if dtype == datetime:
                        df[col] = pd.to_datetime(df[col])
                    else:
                        df[col] = df[col].astype(dtype)
                        
        return df
        
    @staticmethod
    def to_csv_string(
        df: pd.DataFrame,
        include_index: bool = False,
        **kwargs
    ) -> str:
        """Convert DataFrame to CSV string"""
        return df.to_csv(index=include_index, **kwargs)
        
    @staticmethod
    def from_csv_string(
        csv_str: str,
        dtypes: Optional[Dict[str, Type]] = None,
        **kwargs
    ) -> pd.DataFrame:
        """Create DataFrame from CSV string"""
        df = pd.read_csv(io.StringIO(csv_str), **kwargs)
        
        # Apply dtypes
        if dtypes:
            for col, dtype in dtypes.items():
                if col in df.columns:
                    df[col] = df[col].astype(dtype)
                    
        return df


class FormatConverter:
    """
    Universal format converter.
    
    Supports conversion between various data formats.
    """
    
    def __init__(self):
        self._converters: Dict[Tuple[DataFormat, DataFormat], Callable] = {}
        self._register_default_converters()
        
    def _register_default_converters(self):
        """Register default format converters"""
        # JSON conversions
        self.register_converter(DataFormat.JSON, DataFormat.CSV, self._json_to_csv)
        self.register_converter(DataFormat.JSON, DataFormat.YAML, self._json_to_yaml)
        self.register_converter(DataFormat.JSON, DataFormat.XML, self._json_to_xml)
        
        # CSV conversions
        self.register_converter(DataFormat.CSV, DataFormat.JSON, self._csv_to_json)
        self.register_converter(DataFormat.CSV, DataFormat.YAML, self._csv_to_yaml)
        
        # YAML conversions
        self.register_converter(DataFormat.YAML, DataFormat.JSON, self._yaml_to_json)
        self.register_converter(DataFormat.YAML, DataFormat.CSV, self._yaml_to_csv)
        
        # XML conversions
        self.register_converter(DataFormat.XML, DataFormat.JSON, self._xml_to_json)
        self.register_converter(DataFormat.XML, DataFormat.YAML, self._xml_to_yaml)
        
    def register_converter(
        self,
        from_format: DataFormat,
        to_format: DataFormat,
        converter: Callable[[Any], Any]
    ):
        """Register format converter"""
        self._converters[(from_format, to_format)] = converter
        
    def convert(
        self,
        data: Any,
        from_format: DataFormat,
        to_format: DataFormat,
        **kwargs
    ) -> Any:
        """Convert data between formats"""
        if from_format == to_format:
            return data
            
        # Direct conversion
        converter = self._converters.get((from_format, to_format))
        if converter:
            return converter(data, **kwargs)
            
        # Try through JSON as intermediate
        if from_format != DataFormat.JSON and to_format != DataFormat.JSON:
            json_converter = self._converters.get((from_format, DataFormat.JSON))
            from_json_converter = self._converters.get((DataFormat.JSON, to_format))
            
            if json_converter and from_json_converter:
                json_data = json_converter(data, **kwargs)
                return from_json_converter(json_data, **kwargs)
                
        raise ConversionError(
            f"No converter available from {from_format} to {to_format}"
        )
        
    def _json_to_csv(self, data: Union[str, dict, list], **kwargs) -> str:
        """Convert JSON to CSV"""
        if isinstance(data, str):
            data = json.loads(data)
            
        if isinstance(data, dict):
            data = [data]
            
        if not isinstance(data, list):
            raise ConversionError("JSON must be object or array for CSV conversion")
            
        if not data:
            return ""
            
        # Convert to DataFrame
        df = pd.DataFrame(data)
        return DataFrameConverter.to_csv_string(df, **kwargs)
        
    def _csv_to_json(self, data: str, **kwargs) -> str:
        """Convert CSV to JSON"""
        df = DataFrameConverter.from_csv_string(data, **kwargs)
        records = DataFrameConverter.to_dict_records(df)
        return json.dumps(records, default=TypeConverter.to_json_serializable)
        
    def _json_to_yaml(self, data: Union[str, dict, list], **kwargs) -> str:
        """Convert JSON to YAML"""
        if isinstance(data, str):
            data = json.loads(data)
            
        return yaml.dump(data, default_flow_style=False, **kwargs)
        
    def _yaml_to_json(self, data: str, **kwargs) -> str:
        """Convert YAML to JSON"""
        yaml_data = yaml.safe_load(data)
        return json.dumps(yaml_data, default=TypeConverter.to_json_serializable)
        
    def _json_to_xml(self, data: Union[str, dict], root_name: str = "root", **kwargs) -> str:
        """Convert JSON to XML"""
        if isinstance(data, str):
            data = json.loads(data)
            
        def dict_to_xml(tag: str, d: Any) -> ET.Element:
            """Convert dictionary to XML element"""
            elem = ET.Element(tag)
            
            if isinstance(d, dict):
                for key, val in d.items():
                    if isinstance(val, list):
                        for item in val:
                            elem.append(dict_to_xml(key, item))
                    else:
                        elem.append(dict_to_xml(key, val))
            elif isinstance(d, list):
                for item in d:
                    elem.append(dict_to_xml("item", item))
            else:
                elem.text = str(d)
                
            return elem
            
        root = dict_to_xml(root_name, data)
        
        # Pretty print
        xml_str = ET.tostring(root, encoding='unicode')
        dom = minidom.parseString(xml_str)
        return dom.toprettyxml(indent="  ")
        
    def _xml_to_json(self, data: str, **kwargs) -> str:
        """Convert XML to JSON"""
        def xml_to_dict(element: ET.Element) -> Dict[str, Any]:
            """Convert XML element to dictionary"""
            result = {}
            
            # Add attributes
            if element.attrib:
                result["@attributes"] = element.attrib
                
            # Add text content
            if element.text and element.text.strip():
                if len(element) == 0:  # Leaf node
                    return element.text.strip()
                else:
                    result["@text"] = element.text.strip()
                    
            # Add children
            for child in element:
                child_data = xml_to_dict(child)
                
                if child.tag in result:
                    # Convert to list if multiple children with same tag
                    if not isinstance(result[child.tag], list):
                        result[child.tag] = [result[child.tag]]
                    result[child.tag].append(child_data)
                else:
                    result[child.tag] = child_data
                    
            return result if result else None
            
        root = ET.fromstring(data)
        xml_dict = {root.tag: xml_to_dict(root)}
        return json.dumps(xml_dict, default=TypeConverter.to_json_serializable)
        
    def _csv_to_yaml(self, data: str, **kwargs) -> str:
        """Convert CSV to YAML"""
        json_data = self._csv_to_json(data, **kwargs)
        return self._json_to_yaml(json_data, **kwargs)
        
    def _yaml_to_csv(self, data: str, **kwargs) -> str:
        """Convert YAML to CSV"""
        json_data = self._yaml_to_json(data, **kwargs)
        return self._json_to_csv(json_data, **kwargs)
        
    def _xml_to_yaml(self, data: str, **kwargs) -> str:
        """Convert XML to YAML"""
        json_data = self._xml_to_json(data, **kwargs)
        return self._json_to_yaml(json_data, **kwargs)


class BinaryConverter:
    """Binary data conversion utilities"""
    
    @staticmethod
    def to_base64(data: bytes) -> str:
        """Convert bytes to base64 string"""
        return base64.b64encode(data).decode('utf-8')
        
    @staticmethod
    def from_base64(data: str) -> bytes:
        """Convert base64 string to bytes"""
        return base64.b64decode(data.encode('utf-8'))
        
    @staticmethod
    def to_hex(data: bytes) -> str:
        """Convert bytes to hex string"""
        return data.hex()
        
    @staticmethod
    def from_hex(data: str) -> bytes:
        """Convert hex string to bytes"""
        return bytes.fromhex(data)


class SchemaConverter:
    """Schema conversion utilities"""
    
    @staticmethod
    def pandas_to_spark_schema(df: pd.DataFrame) -> List[Dict[str, str]]:
        """Convert pandas DataFrame schema to Spark schema"""
        spark_schema = []
        
        type_mapping = {
            'int64': 'LongType',
            'int32': 'IntegerType',
            'float64': 'DoubleType',
            'float32': 'FloatType',
            'bool': 'BooleanType',
            'object': 'StringType',
            'datetime64[ns]': 'TimestampType',
            'timedelta64[ns]': 'LongType'
        }
        
        for col, dtype in df.dtypes.items():
            spark_type = type_mapping.get(str(dtype), 'StringType')
            spark_schema.append({
                'name': col,
                'type': spark_type,
                'nullable': df[col].isnull().any()
            })
            
        return spark_schema
        
    @staticmethod
    def spark_to_pandas_dtypes(spark_schema: List[Dict[str, str]]) -> Dict[str, Type]:
        """Convert Spark schema to pandas dtypes"""
        type_mapping = {
            'LongType': np.int64,
            'IntegerType': np.int32,
            'DoubleType': np.float64,
            'FloatType': np.float32,
            'BooleanType': bool,
            'StringType': object,
            'TimestampType': 'datetime64[ns]',
            'DateType': 'datetime64[ns]'
        }
        
        dtypes = {}
        for field in spark_schema:
            pandas_type = type_mapping.get(field['type'], object)
            dtypes[field['name']] = pandas_type
            
        return dtypes


# Global converter instance
format_converter = FormatConverter()


def convert_data(
    data: Any,
    from_format: Union[str, DataFormat],
    to_format: Union[str, DataFormat],
    **kwargs
) -> Any:
    """
    Convert data between formats.
    
    Args:
        data: Input data
        from_format: Source format
        to_format: Target format
        **kwargs: Additional conversion options
        
    Returns:
        Converted data
    """
    if isinstance(from_format, str):
        from_format = DataFormat(from_format)
    if isinstance(to_format, str):
        to_format = DataFormat(to_format)
        
    return format_converter.convert(data, from_format, to_format, **kwargs) 